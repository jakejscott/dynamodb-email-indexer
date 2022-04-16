use anyhow::Context;
use aws_sdk_dynamodb::{
    model::{AttributeValue, KeysAndAttributes},
    Client,
};
use dynamodb_email_indexer::email_index_schema::EmailIndexSchema;
use dynamodb_email_indexer::search_response::SearchResponse;
use dynamodb_email_indexer::{email::Email, search_request::SearchRequest};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tantivy::{collector::Count, collector::TopDocs, query::QueryParser, IndexReader};

#[derive(Serialize, Deserialize)]
struct LambdaFunctionUrlRequest {
    body: String,
}

struct Config {
    index_reader: IndexReader,
    email_index_schema: EmailIndexSchema,
    query_parser: QueryParser,
    ddb: Client,
    table_name: String,
    last_reload: Instant,
}

type SharedConfig = Arc<Mutex<Config>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    let table_name = std::env::var("TABLE_NAME").context("TABLE_NAME env var missing")?;
    let config = aws_config::load_from_env().await;
    let ddb = aws_sdk_dynamodb::Client::new(&config);

    let email_index_schema = EmailIndexSchema::new();
    let email_index = email_index_schema.ensure_index()?;

    let index_reader = email_index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::OnCommit)
        .try_into()?;

    let query_parser = QueryParser::for_index(&email_index, email_index_schema.default_fields());

    let config = Config {
        index_reader,
        email_index_schema,
        query_parser,
        ddb,
        table_name,
        last_reload: Instant::now(),
    };

    let shared_config = SharedConfig::new(Mutex::new(config));

    lambda_runtime::run(service_fn(
        |event: LambdaEvent<LambdaFunctionUrlRequest>| async {
            let (event, _context) = event.into_parts();
            info!("event: {}", json!(event));

            let search_request: SearchRequest = serde_json::from_str(event.body.as_str())?;

            let start = Instant::now();
            let config = &mut *shared_config.lock().unwrap();

            if Instant::now() - config.last_reload > Duration::from_secs(3) {
                config.index_reader.reload()?;
                config.last_reload = Instant::now();
            }

            let result = search(config, search_request).await?;

            println!("elapsed: {:?}", start.elapsed());

            return Ok::<SearchResponse, Error>(result);
        },
    ))
    .await?;

    Ok(())
}

async fn search(config: &Config, request: SearchRequest) -> Result<SearchResponse, Error> {
    if request.query.is_none() {
        return Ok(SearchResponse::error("query is required"));
    }

    let query = request.query.unwrap();
    let limit: usize = request.limit.unwrap_or(10);

    match config.query_parser.parse_query(query.as_str()) {
        Ok(query) => {
            let searcher = config.index_reader.searcher();

            let total = searcher.num_docs();
            let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;
            let count = searcher.search(&query, &Count)?;

            let mut ids: Vec<String> = vec![];

            for (_, doc_address) in top_docs {
                let retrieved_doc = searcher.doc(doc_address)?;

                let id = retrieved_doc
                    .get_first(config.email_index_schema.fields.id)
                    .unwrap()
                    .as_text()
                    .unwrap();

                ids.push(id.to_string());
            }

            let emails: Vec<Email> = batch_get_items(config, &ids).await?;

            return Ok(SearchResponse::success(total, count, emails));
        }
        Err(error) => {
            return Ok(SearchResponse::error(error.to_string().as_str()));
        }
    };
}

async fn batch_get_items(config: &Config, ids: &Vec<String>) -> anyhow::Result<Vec<Email>> {
    let mut emails: Vec<Email> = vec![];

    for batch in ids.chunks(100) {
        let mut keys: Vec<HashMap<String, AttributeValue>> = vec![];

        for id in batch {
            let item = HashMap::from([("id".to_owned(), AttributeValue::S(id.to_owned()))]);
            keys.push(item);
        }

        let response = config
            .ddb
            .batch_get_item()
            .request_items(
                config.table_name.clone(),
                KeysAndAttributes::builder().set_keys(Some(keys)).build(),
            )
            .send()
            .await?;

        for response in response.responses() {
            if let Some(rows) = response.get(config.table_name.as_str()) {
                for attributes in rows {
                    let email = Email::from(attributes)?;
                    emails.push(email);
                }
            }
        }
    }

    Ok(emails)
}
