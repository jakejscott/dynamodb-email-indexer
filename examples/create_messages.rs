use anyhow::{Error, Result};
use aws_config::profile::{ProfileFileCredentialsProvider, ProfileFileRegionProvider};
use aws_sdk_dynamodb::model::{PutRequest, WriteRequest};
use ddb_stream_indexer::Message;
use fake::{
    faker::lorem::en::{Paragraph, Sentence},
    Fake,
};
use log::info;
use serde_json::Value;
use std::collections::HashMap;
use tokio::fs;
use ulid::Ulid;

#[tokio::main]
async fn main() -> Result<(), Error> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    let profile_name = "dynamodb-stream-indexer";
    let json = fs::read_to_string("outputs.json").await?;
    let outputs = serde_json::from_str::<Value>(&json)?;

    let table_name = outputs
        .get("dynamodb-stream-indexer")
        .unwrap()
        .get("TableName")
        .unwrap()
        .as_str()
        .unwrap();

    info!("table name: {table_name}");

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(profile_name)
        .build();

    let region_provider = ProfileFileRegionProvider::builder()
        .profile_name(profile_name)
        .build();

    let config = aws_config::from_env()
        .region(region_provider)
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let ddb = aws_sdk_dynamodb::Client::new(&config);

    let mut write_requests: Vec<WriteRequest> = vec![];

    for _ in 1..=10000 {
        let message = Message {
            pk: format!("BLOG#1"),
            sk: format!("POST#{}", Ulid::new()),
            subject: Sentence(1..5).fake(),
            body: Paragraph(1..3).fake::<String>(),
        };

        let put_request = PutRequest::builder()
            .set_item(Some(message.attributes()))
            .build();

        let write_request = WriteRequest::builder().put_request(put_request).build();
        write_requests.push(write_request);
    }

    let total = write_requests.len();

    let mut count = 0;

    for batch in write_requests.chunks(25) {
        info!("Writing batch. Sent {count} of {total}");

        let request_items = HashMap::from([(table_name.to_owned(), batch.to_vec())]);

        ddb.batch_write_item()
            .set_request_items(Some(request_items))
            .send()
            .await?;

        count += batch.len();
    }

    info!("Done");

    Ok(())
}
