use anyhow::Context;
use aws_sdk_dynamodb::{
    model::{AttributeValue, KeysAndAttributes},
    Client,
};
use ddb_stream_indexer::{build_schema, open_index, IndexSchema};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};
use tantivy::{collector::TopDocs, query::QueryParser, IndexReader};

#[derive(Deserialize, Serialize)]
pub struct SearchRequest {
    pub query: Option<String>,
    pub limit: Option<usize>,
}

struct Config {
    index_reader: IndexReader,
    index_schema: IndexSchema,
    query_parser: QueryParser,
    ddb: Client,
    table_name: String,
}

struct MessageId {
    pk: String,
    sk: String,
}

type SharedConfig = Arc<Mutex<Config>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    let table_name = std::env::var("TABLE_NAME").context("TABLE_NAME env var missing")?;

    let config = aws_config::load_from_env().await;
    let ddb = aws_sdk_dynamodb::Client::new(&config);

    let index_schema = build_schema();
    let index = open_index()?;

    let index_reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::OnCommit)
        .try_into()?;

    let query_parser = QueryParser::for_index(
        &index,
        vec![
            index_schema.pk,
            index_schema.sk,
            index_schema.subject,
            index_schema.body,
        ],
    );

    let config = Config {
        index_reader,
        index_schema,
        query_parser,
        ddb,
        table_name,
    };

    let shared_config = SharedConfig::new(Mutex::new(config));

    lambda_runtime::run(service_fn(|event: LambdaEvent<SearchRequest>| async {
        let (event, _context) = event.into_parts();
        info!("event: {}", json!(event));

        let start = Instant::now();

        let config = &*shared_config.lock().unwrap();
        let result = func(config, event).await?;

        println!("elapsed: {:?}", start.elapsed());

        return Ok::<Value, Error>(result);
    }))
    .await?;

    Ok(())
}

async fn func(config: &Config, request: SearchRequest) -> Result<Value, Error> {
    if request.query.is_none() {
        let result = json!({
            "hits": [],
            "error": "query is required"
        });
        return Ok(result);
    }

    let query = request.query.unwrap();
    let limit: usize = request.limit.unwrap_or(10);

    match config.query_parser.parse_query(query.as_str()) {
        Ok(query) => {
            let searcher = config.index_reader.searcher();
            let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

            let mut hits: Vec<Value> = vec![];
            let mut message_ids: Vec<MessageId> = vec![];

            for (_score, doc_address) in top_docs {
                let retrieved_doc = searcher.doc(doc_address)?;

                let json = config.index_schema.schema.to_json(&retrieved_doc);
                println!("hit {}", json);

                let pk = retrieved_doc
                    .get_first(config.index_schema.pk)
                    .unwrap()
                    .as_text()
                    .unwrap();

                let sk = retrieved_doc
                    .get_first(config.index_schema.sk)
                    .unwrap()
                    .as_text()
                    .unwrap();

                let message_id = MessageId {
                    pk: pk.to_string(),
                    sk: sk.to_string(),
                };
                message_ids.push(message_id);

                let value: Value = serde_json::from_str(json.as_str())?;
                hits.push(value)
            }

            for chuck in message_ids.chunks(100) {
                let mut keys: Vec<HashMap<String, AttributeValue>> = vec![];
                for message in chuck {
                    let item: HashMap<String, AttributeValue> = HashMap::from([
                        ("PK".to_owned(), AttributeValue::S(message.pk.to_owned())),
                        ("SK".to_owned(), AttributeValue::S(message.sk.to_owned())),
                    ]);
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

                println!("Batch get output {:?}", response.responses);
            }

            let result = json!({
                "hits": hits,
            });

            return Ok(result);
        }
        Err(_) => {
            println!("Error parsing query {}", query);

            let result = json!({
                "hits": [],
                "error": "error parsing query"
            });

            return Ok(result);
        }
    };
}
