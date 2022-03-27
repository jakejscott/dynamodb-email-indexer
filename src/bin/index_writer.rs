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
use tantivy::{doc, Document, IndexWriter, Term};

struct Config {
    index_schema: IndexSchema,
}

type SharedConfig = Arc<Mutex<Config>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    let index_schema = build_schema();
    let index = ensure_index(&index_schema)?;
    let config = Config { index_schema };
    let shared_config = SharedConfig::new(Mutex::new(config));

    lambda_runtime::run(service_fn(|event: LambdaEvent<Event>| async {
        let (event, _context) = event.into_parts();
        info!("event: {}", json!(event));

        let start = Instant::now();
        let config = &mut *shared_config.lock().unwrap();

        // NOTE: Index writer lock will be released when this function returns
        let mut index_writer = index.writer(50_000_000)?;

        let result = func(config, &mut index_writer, event).await?;

        println!("elapsed: {:?}", start.elapsed());

        return Ok::<(), Error>(result);
    }))
    .await?;

    Ok(())
}

async fn func(
    config: &mut Config,
    index_writer: &mut IndexWriter,
    event: Event,
) -> Result<(), Error> {
    let total = event.records.len() as u32;

    let mut created = 0_u32;
    let mut updated = 0_u32;
    let mut deleted = 0_u32;

    for record in event.records {
        match record.event_name.as_str() {
            "INSERT" => {
                if let Some(doc) = parse_document(config, record.change.new_image) {
                    info!("creating document");
                    index_writer.add_document(doc)?;
                    created += 1;
                }
            }
            "MODIFY" => {
                if let Some(doc) = parse_document(config, record.change.new_image) {
                    info!("updating document");
                    let term = get_pksk_term(config, &doc);
                    index_writer.delete_term(term);
                    index_writer.add_document(doc)?;
                    updated += 1;
                }
            }
            "REMOVE" => {
                if let Some(doc) = parse_document(config, record.change.old_image) {
                    info!("deleting document");
                    let term = get_pksk_term(config, &doc);
                    index_writer.delete_term(term);
                    deleted += 1;
                }
            }
            _ => {}
        }
    }

    info!("commiting index");
    index_writer.commit()?;

    let result = json!({
        "total": total,
        "created": created,
        "updated": updated,
        "deleted": deleted,
        "skipped": total - created - updated - deleted,
    });

    info!("indexed {}", result);

    Ok(())
}

fn get_pksk_term(config: &Config, doc: &Document) -> Term {
    let pksk = doc
        .get_first(config.index_schema.pksk)
        .expect("Documents should have a pksk value")
        .as_text()
        .expect("pksk value should be text");

    let term = Term::from_field_text(config.index_schema.pksk, pksk);
    term
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

    let pk = pk_value.expect("We have checked that we have a pk_value");
    let sk = sk_value.expect("We have checked that we have a sk_value");
    let pksk = format!("{}:{}", pk, sk);

    let doc = doc!(
        config.index_schema.pksk => pksk,
        config.index_schema.pk => pk,
        config.index_schema.sk => sk,
        config.index_schema.subject => subject_value.expect("We have checked that we have a subject"),
        config.index_schema.body => body_value.expect("We have check that we have a body")
    );

    Some(doc)
}
