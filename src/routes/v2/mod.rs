mod transactions;

use axum::Router;
use axum::routing::get;

use crate::handler::live::live_transfers_handler;
use crate::handler::transfers::list_transfers_handler;
use crate::state::AppState;

pub fn router(live_feed_enabled: bool) -> Router<AppState> {
    let router = Router::new()
        .nest("/transactions", transactions::router())
        .route("/transfers", get(list_transfers_handler));

    // Off means the route does not exist: no Redis command, a plain 404.
    if live_feed_enabled {
        router.route("/transfers/live", get(live_transfers_handler))
    } else {
        router
    }
}
