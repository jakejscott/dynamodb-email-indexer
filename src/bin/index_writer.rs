use aws_lambda_events::dynamodb::{attributes::AttributeValue, Event};
use ddb_stream_indexer::{build_schema, ensure_index, IndexSchema};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::info;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};
use tantivy::{doc, Document, IndexWriter};

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

        println!("elapsed: {:?}", start.elapsed());
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

        if let Some(doc) = parse_document(config, new_image) {
            info!("writing document");
            config.index_writer.add_document(doc)?;
        }

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

fn parse_document(config: &Config, new_image: HashMap<String, AttributeValue>) -> Option<Document> {
    let pk_value: Option<&str> = if let Some(attr) = new_image.get("PK") {
        if let AttributeValue::String(value) = attr {
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

    let sk_value: Option<&str> = if let Some(attr) = new_image.get("SK") {
        if let AttributeValue::String(value) = attr {
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

    let subject_value: Option<&str> = if let Some(attr) = new_image.get("subject") {
        if let AttributeValue::String(value) = attr {
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

    let body_value: Option<&str> = if let Some(attr) = new_image.get("body") {
        if let AttributeValue::String(value) = attr {
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

    let doc = doc!(
        config.index_schema.pk => pk_value.unwrap(),
        config.index_schema.sk => sk_value.unwrap(),
        config.index_schema.subject => subject_value.unwrap(),
        config.index_schema.body => body_value.unwrap()
    );

    Some(doc)
}
