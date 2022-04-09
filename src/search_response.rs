use crate::email::Email;

use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct SearchResponse {
    pub index_num_docs: Option<u64>,
    pub query_num_docs: Option<usize>,
    pub emails: Option<Vec<Email>>,
    pub error: Option<String>,
}

impl SearchResponse {
    pub fn error(error: &str) -> Self {
        SearchResponse {
            error: Some(error.to_string()),
            ..Default::default()
        }
    }

    pub fn success(total: u64, count: usize, emails: Vec<Email>) -> Self {
        SearchResponse {
            index_num_docs: Some(total),
            query_num_docs: Some(count),
            emails: Some(emails),
            error: None,
        }
    }
}
