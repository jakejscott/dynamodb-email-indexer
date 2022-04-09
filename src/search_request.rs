use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct SearchRequest {
    pub query: Option<String>,
    pub limit: Option<usize>,
}
