//! Dashboard server modules.

pub mod dashboard_api;
pub mod http_server;
pub use dashboard_api::DashboardApi;
pub use http_server::{build_router, run_dashboard_server};
