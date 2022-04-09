use aws_lambda_events::dynamodb::{attributes::AttributeValue, Event};
use dynamodb_email_indexer::email_index_schema::EmailIndexSchema;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::{debug, info};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};
use tantivy::{doc, Document, IndexWriter, Term};
struct Config {
    email_index_schema: EmailIndexSchema,
}

type SharedConfig = Arc<Mutex<Config>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    let email_index_schema = EmailIndexSchema::new();
    let email_index = email_index_schema.create()?;
    let config = Config { email_index_schema };
    let shared_config = SharedConfig::new(Mutex::new(config));

    lambda_runtime::run(service_fn(|event: LambdaEvent<Event>| async {
        let (event, _context) = event.into_parts();
        let start = Instant::now();

        let config = &mut *shared_config.lock().unwrap();
        let mut index_writer = email_index.writer(200_000_000)?;

        let result = index_write(config, &mut index_writer, event).await?;

        index_writer.wait_merging_threads()?;
        println!("elapsed: {:?}", start.elapsed());

        return Ok::<(), Error>(result);
    }))
    .await?;

    Ok(())
}

async fn index_write(
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
                let doc = parse_document(config, record.change.new_image)?;
                debug!("creating document");
                index_writer.add_document(doc)?;
                created += 1;
            }
            "MODIFY" => {
                let doc = parse_document(config, record.change.new_image)?;
                debug!("updating document");
                let term = get_id_term(config, &doc);
                index_writer.delete_term(term);
                index_writer.add_document(doc)?;
                updated += 1;
            }
            "REMOVE" => {
                let doc = parse_document(config, record.change.old_image)?;
                debug!("deleting document");
                let term = get_id_term(config, &doc);
                index_writer.delete_term(term);
                deleted += 1;
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

fn get_id_term(config: &Config, doc: &Document) -> Term {
    let id = doc
        .get_first(config.email_index_schema.fields.id)
        .expect("Documents should have a id value")
        .as_text()
        .expect("id value should be text");

    let term = Term::from_field_text(config.email_index_schema.fields.id, id);
    term
}

fn parse_document(
    config: &Config,
    attributes: HashMap<String, AttributeValue>,
) -> anyhow::Result<Document> {
    let id = parse_string(&attributes, "id")?;
    let timestamp: i64 = parse_string(&attributes, "timestamp")?.parse()?;
    let subject = parse_string(&attributes, "subject")?;
    let body = parse_string(&attributes, "body")?;
    let to = parse_string_array(&attributes, "to")?;

    let mut doc = doc!(
        config.email_index_schema.fields.id => id,
        config.email_index_schema.fields.timestamp => timestamp,
        config.email_index_schema.fields.subject => subject,
        config.email_index_schema.fields.body => body,
    );

    for email in to {
        doc.add_text(config.email_index_schema.fields.to, email);
    }

    Ok(doc)
}

pub fn parse_string(
    attributes: &HashMap<String, AttributeValue>,
    attribute_name: &str,
) -> anyhow::Result<String> {
    if let Some(attr) = attributes.get(attribute_name) {
        if let AttributeValue::String(value) = attr {
            return Ok(value.clone());
        }
    }

    Err(anyhow::anyhow!("{attribute_name} missing"))
}

pub fn parse_string_array(
    attributes: &HashMap<String, AttributeValue>,
    attribute_name: &str,
) -> anyhow::Result<Vec<String>> {
    if let Some(attr) = attributes.get(attribute_name) {
        if let AttributeValue::StringSet(values) = attr {
            return Ok(values.clone());
        }
    };

    Err(anyhow::anyhow!("{attribute_name} missing"))
}
