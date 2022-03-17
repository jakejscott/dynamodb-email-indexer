use aws_lambda_events::dynamodb::{attributes::AttributeValue, Event};
use ddb_stream_indexer::{build_schema, ensure_index, IndexSchema};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::info;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tantivy::{doc, IndexWriter};

struct Config {
    index_writer: IndexWriter,
    index_schema: IndexSchema,
}

type SharedConfig = Arc<Mutex<Config>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    let index_schema = build_schema();
    let index = ensure_index(&index_schema)?;

    let index_writer = index.writer(50_000_000)?; // 50MB

    let config = Config {
        index_writer,
        index_schema,
    };

    let shared_config = SharedConfig::new(Mutex::new(config));

    lambda_runtime::run(service_fn(|event: LambdaEvent<Event>| async {
        let (event, _context) = event.into_parts();
        info!("event: {}", json!(event));

        let start = Instant::now();

        let config = &mut *shared_config.lock().unwrap();
        let result = func(config, event).await?;

        let end = Instant::now();
        let time = end - start;

        println!("elapsed: {:?}", time);
        return Ok::<(), Error>(result);
    }))
    .await?;

    Ok(())
}

async fn func(config: &mut Config, event: Event) -> Result<(), Error> {
    let total = event.records.len() as u32;
    let mut indexed = 0_u32;

    for record in event.records {
        let new_image = record.change.new_image;

        let pk_value: Option<&str> = if let Some(attr) = new_image.get("PK") {
            match attr {
                AttributeValue::String(value) => Some(value.as_str()),
                _ => None,
            }
        } else {
            None
        };

        if pk_value.is_none() {
            info!("PK missing");
            continue;
        }

        let sk_value: Option<&str> = if let Some(attr) = new_image.get("SK") {
            match attr {
                AttributeValue::String(value) => Some(value.as_str()),
                _ => None,
            }
        } else {
            None
        };

        if sk_value.is_none() {
            info!("SK missing");
            continue;
        }

        let subject_value: Option<&str> = if let Some(attr) = new_image.get("subject") {
            match attr {
                AttributeValue::String(value) => Some(value.as_str()),
                _ => None,
            }
        } else {
            None
        };

        if subject_value.is_none() {
            info!("subject missing");
            continue;
        }

        let body_value: Option<&str> = if let Some(attr) = new_image.get("body") {
            match attr {
                AttributeValue::String(value) => Some(value.as_str()),
                _ => None,
            }
        } else {
            None
        };

        if body_value.is_none() {
            info!("body missing");
            continue;
        }

        info!("writing document");

        config.index_writer.add_document(doc!(
            config.index_schema.pk => pk_value.unwrap(),
            config.index_schema.sk => sk_value.unwrap(),
            config.index_schema.subject => subject_value.unwrap(),
            config.index_schema.body => body_value.unwrap()
        ))?;

        indexed += 1;
    }

    info!("commiting index");
    config.index_writer.commit()?;

    let result = json!({
        "total": total,
        "indexed": indexed,
        "skipped": total - indexed,
    });

    info!("indexed {}", result);

    Ok(())
}
