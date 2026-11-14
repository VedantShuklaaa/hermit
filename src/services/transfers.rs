//! `GET /api/v2/transfers`: one keyset page over `bridge_transfers`, newest first.
//!
//! The cursor is opaque (base64 of `${created_at}|${id}` as JSON) and exclusive, so
//! paging never repeats or skips a row, including rows that share `created_at`. There
//! is no `OFFSET` and no total count.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::chains::{ChainKey, Snapshot};
use crate::db::repository::{self, TransferListQuery};
use crate::domain::api::{TransferListResponse, TransferStatus};
use crate::error::AppError;
use crate::protocols::Protocol;
use crate::services::api_v2;

/// Default page size when `limit` is omitted.
pub const DEFAULT_LIMIT: u32 = 50;
/// Largest page a caller may ask for.
pub const MAX_LIMIT: u32 = 200;

/// Raw query parameters. Everything stays a string so this module, not axum's
/// deserializer, produces the 400 message.
#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    pub limit: Option<String>,
    pub cursor: Option<String>,
    pub protocol: Option<String>,
    pub direction: Option<String>,
    pub status: Option<String>,
    pub chain: Option<String>,
    pub token: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

/// The keyset position. `created_at` and `id` together are unique.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub created_at: DateTime<Utc>,
    pub id: i64,
}

impl Cursor {
    pub fn encode(&self) -> String {
        // Serialization of a plain struct with two fields cannot fail.
        let json = serde_json::to_vec(self).expect("cursor serializes");
        URL_SAFE_NO_PAD.encode(json)
    }

    pub fn decode(raw: &str) -> Result<Self, AppError> {
        let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| invalid_cursor())?;
        serde_json::from_slice(&bytes).map_err(|_| invalid_cursor())
    }
}

fn invalid_cursor() -> AppError {
    AppError::BadRequest("invalid cursor".to_string())
}

/// Validated parameters, with the chain slug already resolved.
#[derive(Debug)]
pub struct ListParams {
    pub limit: u32,
    pub cursor: Option<Cursor>,
    pub protocol: Option<String>,
    pub direction: Option<String>,
    pub status: Option<String>,
    pub chain_id: Option<i16>,
    pub token: Option<String>,
    /// Inclusive lower bound on `created_at`.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive upper bound on `created_at`.
    pub to: Option<DateTime<Utc>>,
}

pub fn parse_query(raw: ListQuery, chains: &Snapshot) -> Result<ListParams, AppError> {
    let limit = match raw.limit.as_deref() {
        None => DEFAULT_LIMIT,
        Some(value) => {
            let parsed = value.parse::<u32>().map_err(|_| {
                AppError::BadRequest(format!(
                    "limit must be an integer between 1 and {MAX_LIMIT}"
                ))
            })?;
            if parsed == 0 || parsed > MAX_LIMIT {
                return Err(AppError::BadRequest(format!(
                    "limit must be between 1 and {MAX_LIMIT}"
                )));
            }
            parsed
        }
    };

    let cursor = match raw.cursor.as_deref() {
        None => None,
        Some(value) => Some(Cursor::decode(value)?),
    };

    let protocol = match non_empty("protocol", raw.protocol)? {
        None => None,
        Some(value) if Protocol::parse(&value).is_some() => Some(value),
        Some(_) => {
            return Err(AppError::BadRequest(
                "protocol must be wormhole, cctp, debridge or mayan".to_string(),
            ));
        }
    };

    let token = non_empty("token", raw.token)?;

    let direction = match non_empty("direction", raw.direction)? {
        None => None,
        Some(value) if value == "Outbound" || value == "Inbound" => Some(value),
        Some(_) => {
            return Err(AppError::BadRequest(
                "direction must be Outbound or Inbound".to_string(),
            ));
        }
    };

    let status = match non_empty("status", raw.status)? {
        None => None,
        Some(value) if TransferStatus::parse(&value).is_some() => Some(value),
        Some(_) => {
            return Err(AppError::BadRequest(
                "status must be Detected, Pending, Completed, Failed or Refunded".to_string(),
            ));
        }
    };

    let chain_id = match non_empty("chain", raw.chain)? {
        None => None,
        Some(slug) => {
            let ChainKey(id) = chains
                .chain_id_by_slug(&slug)
                .ok_or_else(|| AppError::BadRequest(format!("unknown chain: {slug}")))?;
            Some(id)
        }
    };

    let from = match non_empty("from", raw.from)? {
        None => None,
        Some(value) => Some(parse_timestamp("from", &value)?),
    };
    let to = match non_empty("to", raw.to)? {
        None => None,
        Some(value) => Some(parse_timestamp("to", &value)?),
    };
    if let (Some(from_bound), Some(to_bound)) = (&from, &to)
        && from_bound >= to_bound
    {
        return Err(AppError::BadRequest("from must be before to".to_string()));
    }

    Ok(ListParams {
        limit,
        cursor,
        protocol,
        direction,
        status,
        chain_id,
        token,
        from,
        to,
    })
}

fn non_empty(name: &str, value: Option<String>) -> Result<Option<String>, AppError> {
    match value {
        None => Ok(None),
        Some(value) if value.is_empty() => {
            Err(AppError::BadRequest(format!("{name} must not be empty")))
        }
        Some(value) => Ok(Some(value)),
    }
}

fn parse_timestamp(name: &str, value: &str) -> Result<DateTime<Utc>, AppError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| AppError::BadRequest(format!("{name} must be an RFC3339 timestamp")))
}

/// Fetches one page and maps it with the same serializer `/api/v2/transactions/analyse`
/// uses, so item field names cannot drift.
pub async fn list_transfers(
    pool: &PgPool,
    chains: &Snapshot,
    raw: ListQuery,
) -> Result<TransferListResponse, AppError> {
    let params = parse_query(raw, chains)?;
    let limit = i64::from(params.limit);

    let mut rows = repository::list_transfers(
        pool,
        &TransferListQuery {
            limit,
            cursor: params.cursor.map(|cursor| (cursor.created_at, cursor.id)),
            protocol: params.protocol,
            direction: params.direction,
            status: params.status,
            chain_id: params.chain_id,
            token: params.token,
            from: params.from,
            to: params.to,
        },
    )
    .await
    .map_err(AppError::Internal)?;

    // `limit + 1` rows means there is another page; the extra row is not returned.
    let has_more = rows.len() > params.limit as usize;
    if has_more {
        rows.pop();
    }
    let next_cursor = if has_more {
        rows.last().map(|row| {
            Cursor {
                created_at: row.created_at,
                id: row.id,
            }
            .encode()
        })
    } else {
        None
    };

    let items = rows
        .iter()
        .map(|row| api_v2::transfer_view(&row.transfer, chains))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TransferListResponse { items, next_cursor })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::{ChainRow, RegistryStatus, Snapshot};

    fn snapshot() -> Snapshot {
        Snapshot::build(
            vec![ChainRow {
                id: 1,
                slug: "solana".to_string(),
                name: "Solana".to_string(),
                kind: "solana".to_string(),
                explorer_tx_url: None,
                status: RegistryStatus::Active,
            }],
            vec![],
        )
    }

    fn query() -> ListQuery {
        ListQuery::default()
    }

    fn bad_request(result: Result<ListParams, AppError>) -> String {
        match result {
            Err(AppError::BadRequest(message)) => message,
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    #[test]
    fn the_cursor_round_trips_at_microsecond_precision() {
        let cursor = Cursor {
            created_at: DateTime::from_timestamp_micros(1_700_000_000_123_456).unwrap(),
            id: 9_007_199_254_740_993,
        };
        assert_eq!(Cursor::decode(&cursor.encode()).unwrap(), cursor);
    }

    #[test]
    fn a_cursor_is_url_safe_and_opaque() {
        let cursor = Cursor {
            created_at: DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap(),
            id: 1,
        };
        let encoded = cursor.encode();
        assert!(!encoded.contains('+'));
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains('='));
        assert_ne!(encoded, "2023-11-14T22:13:20Z:1");
    }

    #[test]
    fn invalid_cursors_are_400s() {
        for raw in ["not base64!", "", "e30"] {
            // `e30` is base64 for `{}`, which is not a cursor.
            let message = bad_request(parse_query(
                ListQuery {
                    cursor: Some(raw.to_string()),
                    ..query()
                },
                &snapshot(),
            ));
            assert_eq!(message, "invalid cursor");
        }
    }

    #[test]
    fn limit_defaults_to_fifty_and_is_bounded() {
        let params = parse_query(query(), &snapshot()).unwrap();
        assert_eq!(params.limit, DEFAULT_LIMIT);

        for value in ["1", "200"] {
            let params = parse_query(
                ListQuery {
                    limit: Some(value.to_string()),
                    ..query()
                },
                &snapshot(),
            )
            .unwrap();
            assert_eq!(params.limit, value.parse::<u32>().unwrap());
        }
        for value in ["0", "201", "abc", "-1", ""] {
            let message = bad_request(parse_query(
                ListQuery {
                    limit: Some(value.to_string()),
                    ..query()
                },
                &snapshot(),
            ));
            assert!(message.contains("limit"), "{value}: {message}");
        }
    }

    #[test]
    fn direction_and_status_accept_only_their_enum_values() {
        for value in ["Outbound", "Inbound"] {
            assert!(
                parse_query(
                    ListQuery {
                        direction: Some(value.to_string()),
                        ..query()
                    },
                    &snapshot()
                )
                .is_ok()
            );
        }
        assert!(
            bad_request(parse_query(
                ListQuery {
                    direction: Some("outbound".to_string()),
                    ..query()
                },
                &snapshot()
            ))
            .contains("direction")
        );

        for value in ["Detected", "Pending", "Completed", "Failed", "Refunded"] {
            assert!(
                parse_query(
                    ListQuery {
                        status: Some(value.to_string()),
                        ..query()
                    },
                    &snapshot()
                )
                .is_ok()
            );
        }
        let message = bad_request(parse_query(
            ListQuery {
                status: Some("Done".to_string()),
                ..query()
            },
            &snapshot(),
        ));
        assert!(message.contains("status"));
    }

    #[test]
    fn protocol_accepts_every_known_adapter_and_rejects_unknown_values() {
        for value in ["wormhole", "cctp", "debridge", "mayan"] {
            let params = parse_query(
                ListQuery {
                    protocol: Some(value.to_string()),
                    ..query()
                },
                &snapshot(),
            )
            .unwrap();
            assert_eq!(params.protocol.as_deref(), Some(value));
        }

        let message = bad_request(parse_query(
            ListQuery {
                protocol: Some("stargate".to_string()),
                ..query()
            },
            &snapshot(),
        ));
        assert_eq!(
            message,
            "protocol must be wormhole, cctp, debridge or mayan"
        );
    }

    #[test]
    fn a_chain_slug_resolves_to_its_registry_id() {
        let params = parse_query(
            ListQuery {
                chain: Some("solana".to_string()),
                ..query()
            },
            &snapshot(),
        )
        .unwrap();
        assert_eq!(params.chain_id, Some(1));

        let message = bad_request(parse_query(
            ListQuery {
                chain: Some("atlantis".to_string()),
                ..query()
            },
            &snapshot(),
        ));
        assert_eq!(message, "unknown chain: atlantis");
    }

    #[test]
    fn timestamps_are_rfc3339_and_from_must_precede_to() {
        let params = parse_query(
            ListQuery {
                from: Some("2026-10-01T00:00:00Z".to_string()),
                to: Some("2026-10-31T00:00:00Z".to_string()),
                ..query()
            },
            &snapshot(),
        )
        .unwrap();
        assert_eq!(
            params.from.unwrap(),
            DateTime::from_timestamp_micros(1_790_812_800_000_000).unwrap()
        );
        assert_eq!(
            params.to.unwrap(),
            DateTime::from_timestamp_micros(1_793_404_800_000_000).unwrap()
        );

        let cases: [(Option<&str>, Option<&str>); 4] = [
            (Some("nope"), None),
            (None, Some("nope")),
            (Some("2026-10-31T00:00:00Z"), Some("2026-10-31T00:00:00Z")),
            (Some("2026-10-31T00:00:00Z"), Some("2026-10-01T00:00:00Z")),
        ];
        for (from, to) in cases {
            let message = bad_request(parse_query(
                ListQuery {
                    from: from.map(str::to_string),
                    to: to.map(str::to_string),
                    ..query()
                },
                &snapshot(),
            ));
            assert!(
                message.contains("timestamp") || message.contains("before"),
                "{message}"
            );
        }
    }

    #[test]
    fn empty_filter_values_are_400s() {
        for name in [
            "protocol",
            "direction",
            "status",
            "chain",
            "token",
            "from",
            "to",
        ] {
            let raw = match name {
                "protocol" => ListQuery {
                    protocol: Some(String::new()),
                    ..query()
                },
                "direction" => ListQuery {
                    direction: Some(String::new()),
                    ..query()
                },
                "status" => ListQuery {
                    status: Some(String::new()),
                    ..query()
                },
                "chain" => ListQuery {
                    chain: Some(String::new()),
                    ..query()
                },
                "token" => ListQuery {
                    token: Some(String::new()),
                    ..query()
                },
                "from" => ListQuery {
                    from: Some(String::new()),
                    ..query()
                },
                "to" => ListQuery {
                    to: Some(String::new()),
                    ..query()
                },
                other => unreachable!("{other}"),
            };
            let message = bad_request(parse_query(raw, &snapshot()));
            assert_eq!(message, format!("{name} must not be empty"));
        }
    }
}
