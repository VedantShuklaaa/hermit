//! EVM log watching for the Wormhole Token Bridge. One watcher per configured chain
//! reads the Core Bridge `LogMessagePublished` and Token Bridge `TransferRedeemed`
//! logs, stores them keyed by message identity, and joins them to `bridge_transfers`.
//!
//! This path replaces WormholeScan for tx hashes: WormholeScan stays only as the
//! fallback for chains without a watcher and for events older than the watcher.

pub mod client;
pub mod db;
pub mod decode;
pub mod lookup;
pub mod watcher;
