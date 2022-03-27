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
    time::{Duration, Instant},
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
    last_reload: Instant,
}

#[derive(Serialize)]
struct MessageId {
    score: f32,
    pk: String,
    sk: String,
}

#[derive(Serialize)]
struct Message {
    pk: String,
    sk: String,
    subject: String,
    body: String,
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
        last_reload: Instant::now(),
    };

    let shared_config = SharedConfig::new(Mutex::new(config));

    lambda_runtime::run(service_fn(|event: LambdaEvent<SearchRequest>| async {
        let (event, _context) = event.into_parts();
        info!("event: {}", json!(event));

        let start = Instant::now();

        let config = &mut *shared_config.lock().unwrap();

        if Instant::now() - config.last_reload > Duration::from_secs(30) {
            info!("reload reader");
            config.index_reader.reload()?;
            config.last_reload = Instant::now();
        }

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

            let mut message_ids: Vec<MessageId> = vec![];

            for (score, doc_address) in top_docs {
                let retrieved_doc = searcher.doc(doc_address)?;

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

                message_ids.push(MessageId {
                    score,
                    pk: pk.to_string(),
                    sk: sk.to_string(),
                });
            }

            let messages: Vec<Message> = fetch_messages(config, &message_ids).await?;

            let result = json!({
                "hits": message_ids,
                "messages": messages,
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

async fn fetch_messages(
    config: &Config,
    message_ids: &Vec<MessageId>,
) -> anyhow::Result<Vec<Message>> {
    let mut messages: Vec<Message> = vec![];

    for batch in message_ids.chunks(100) {
        let mut keys: Vec<HashMap<String, AttributeValue>> = vec![];

        for message in batch {
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

        for response in response.responses() {
            if let Some(rows) = response.get(config.table_name.as_str()) {
                for attributes in rows {
                    if let Some(message) = parse_message(attributes) {
                        messages.push(message);
                    }
                }
            }
        }
    }

    Ok(messages)
}

fn parse_message(attributes: &HashMap<String, AttributeValue>) -> Option<Message> {
    let pk_value: Option<&str> = if let Some(attr) = attributes.get("PK") {
        if let AttributeValue::S(value) = attr {
            Some(value.as_str())
        } else {
            None
        }
    } else {
        None
    };

    if pk_value.is_none() {
        info!("PK missing");
        return None;
    }

    let sk_value: Option<&str> = if let Some(attr) = attributes.get("SK") {
        if let AttributeValue::S(value) = attr {
            Some(value.as_str())
        } else {
            None
        }
    } else {
        None
    };

    if sk_value.is_none() {
        info!("SK missing");
        return None;
    }

    let subject_value: Option<&str> = if let Some(attr) = attributes.get("subject") {
        if let AttributeValue::S(value) = attr {
            Some(value.as_str())
        } else {
            None
        }
    } else {
        None
    };

    if subject_value.is_none() {
        info!("subject missing");
        return None;
    }

    let body_value: Option<&str> = if let Some(attr) = attributes.get("body") {
        if let AttributeValue::S(value) = attr {
            Some(value.as_str())
        } else {
            None
        }
    } else {
        None
    };

    if body_value.is_none() {
        info!("body missing");
        return None;
    }

    let message = Message {
        pk: pk_value.unwrap().to_string(),
        sk: sk_value.unwrap().to_string(),
        body: body_value.unwrap().to_string(),
        subject: subject_value.unwrap().to_string(),
    };

    Some(message)
}
