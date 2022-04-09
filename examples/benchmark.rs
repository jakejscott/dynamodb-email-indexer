use anyhow::{Error, Result};
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_sdk_dynamodb::{
    model::{PutRequest, WriteRequest},
    Region,
};
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, SignableRequest, SignatureLocation, SigningParams, SigningSettings,
};
use dynamodb_email_indexer::{
    email::Email, search_request::SearchRequest, search_response::SearchResponse,
};
use fake::{
    faker::{
        internet::en::FreeEmailProvider,
        lorem::en::{Paragraph, Sentence},
        name::en::{FirstName, LastName},
    },
    Fake,
};
use http;
use log::{debug, info};
use serde_json::{json, Value};
use std::time::SystemTime;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use tantivy::chrono::Utc;
use tokio::fs;
use ulid::Ulid;
use xshell::{cmd, Shell};

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// How many emails to create
    #[structopt(short, long)]
    how_many: u64,

    /// AWS credentials profile name
    #[structopt(short, long)]
    profile: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    std::env::set_var("RUST_LOG", "benchmark=info");
    env_logger::init();

    let options = Opt::from_args();
    let profile = options.profile;

    // NOTE: read the aws access key and secret from the profile
    let sh = Shell::new()?;

    let access_key = cmd!(
        sh,
        "aws configure get aws_access_key_id --profile {profile}"
    )
    .read()?;

    let secret_key = cmd!(
        sh,
        "aws configure get aws_secret_access_key --profile {profile}"
    )
    .read()?;

    let region = cmd!(sh, "aws configure get region --profile {profile}").read()?;

    let start = Instant::now();

    let json = fs::read_to_string("outputs.json").await?;
    let outputs = serde_json::from_str::<Value>(&json)?;

    let email_table_name = outputs
        .get(&profile)
        .unwrap()
        .get("EmailTableName")
        .unwrap()
        .as_str()
        .unwrap();

    let email_index_reader_function_url = outputs
        .get(&profile)
        .unwrap()
        .get("EmailIndexReaderFunctionUrl")
        .unwrap()
        .as_str()
        .unwrap();

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(&profile)
        .build();

    let config = aws_config::from_env()
        .region(Region::new(region))
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let ddb = aws_sdk_dynamodb::Client::new(&config);

    // NOTE: Get a count of all the docs in the index before starting...
    let search_response = search_docs(
        email_index_reader_function_url,
        &access_key,
        &secret_key,
        SearchRequest {
            limit: Some(1),
            query: Some("*".to_string()),
        },
    )
    .await?;

    info!(
        "index num docs before starting: {}",
        search_response.index_num_docs.unwrap_or(0)
    );

    let index_count_start = search_response.index_num_docs.unwrap_or(0);

    let mut write_requests: Vec<WriteRequest> = vec![];

    for _ in 1..=options.how_many {
        let mut to: Vec<String> = vec![];

        for _ in 0..1 {
            let first_name: String = FirstName().fake();
            let last_name: String = LastName().fake();
            let domain: String = FreeEmailProvider().fake();
            let email = format!(
                "\"{} {}\" <{}.{}@{}>",
                first_name,
                last_name,
                first_name.to_lowercase(),
                last_name.to_lowercase(),
                domain
            );
            to.push(email);
        }

        let email = Email {
            id: Ulid::new().to_string(),
            timestamp: Utc::now().timestamp(),
            subject: Sentence(1..5).fake(),
            body: Paragraph(1..3).fake::<String>(),
            to: to,
        };

        debug!("email {:?}", email);

        let put_request = PutRequest::builder()
            .set_item(Some(email.attributes()))
            .build();

        let write_request = WriteRequest::builder().put_request(put_request).build();
        write_requests.push(write_request);
    }

    let total = write_requests.len();
    let mut count = 0;
    for batch in write_requests.chunks(25) {
        let request_items = HashMap::from([(email_table_name.to_owned(), batch.to_vec())]);

        ddb.batch_write_item()
            .set_request_items(Some(request_items))
            .send()
            .await?;

        count += batch.len();
        info!("sent {count} of {total}");
    }

    info!("checking total docs count");

    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;

        let search_response = search_docs(
            email_index_reader_function_url,
            &access_key,
            &secret_key,
            SearchRequest {
                limit: Some(1),
                query: Some("*".to_string()),
            },
        )
        .await?;

        let total_indexed =
            search_response.index_num_docs.unwrap_or(0) as i64 - (index_count_start as i64);

        info!(
            "index num docs: {} total indexed: {} elapsed: {:?}",
            search_response.index_num_docs.unwrap_or(0),
            total_indexed,
            start.elapsed()
        );

        if total_indexed >= options.how_many as i64 {
            break;
        }
    }

    info!("done: {:?}", start.elapsed());

    Ok(())
}

async fn search_docs(
    url: &str,
    access_key: &String,
    secret_key: &String,
    search_request: SearchRequest,
) -> Result<SearchResponse> {
    let body = json!(&search_request).to_string();

    // NOTE: The lambda function url is secured using IAM_AUTH, so we need to sign the request using aws v4 sig
    let mut request = http::Request::builder()
        .uri(url)
        .method("POST")
        .header("Content-Type", "application/json")
        .body(body.clone())
        .unwrap();

    let mut signing_settings = SigningSettings::default();
    signing_settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    signing_settings.signature_location = SignatureLocation::Headers;

    let signing_params = SigningParams::builder()
        .access_key(access_key)
        .secret_key(secret_key)
        .region("ap-southeast-2")
        .service_name("lambda")
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()
        .unwrap();

    let signable_request = SignableRequest::from(&request);
    let (signing_instructions, _signature) = sign(signable_request, &signing_params)
        .unwrap()
        .into_parts();

    signing_instructions.apply_to_request(&mut request);

    let client = reqwest::Client::new();

    let mut builder = client.post(url);
    for (name, value) in request.headers() {
        builder = builder.header(name.as_str(), value.to_str().unwrap());
    }

    let response = builder.body(body).send().await?;
    if response.status().is_success() {
        let search_response = response.json::<SearchResponse>().await?;
        return Ok(search_response);
    }

    let text = response.text().await?;
    println!("Error searching docs {}", text);
    return Err(anyhow::anyhow!(text));
}
