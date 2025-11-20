pub mod stateless;
pub mod worker;

use axum::{Router, routing::post};

use crate::state::AppState;

pub fn create_router() -> Router<AppState> {
    Router::new().route("/v1/stateless/run", post(stateless::run_stateless))
}
