use anyhow::{Error, Result};
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, SignableRequest, SignatureLocation, SigningParams, SigningSettings,
};
use dynamodb_email_indexer::{search_request::SearchRequest, search_response::SearchResponse};
use http;
use log::info;
use serde_json::{json, Value};
use std::time::Instant;
use std::time::SystemTime;
use structopt::StructOpt;
use tokio::fs;
use xshell::{cmd, Shell};

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// Search query string
    #[structopt(short, long)]
    query: String,

    /// Search query string
    #[structopt(short, long)]
    limit: Option<usize>,

    /// AWS credentials profile name
    #[structopt(short, long)]
    profile: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    std::env::set_var("RUST_LOG", "search=info");
    env_logger::init();

    let options = Opt::from_args();
    let profile = options.profile;
    let query = options.query;
    let limit = options.limit.unwrap_or(100);

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

    let start = Instant::now();
    let json = fs::read_to_string("outputs.json").await?;
    let outputs = serde_json::from_str::<Value>(&json)?;

    let email_index_reader_function_url = outputs
        .get(&profile)
        .unwrap()
        .get("EmailIndexReaderFunctionUrl")
        .unwrap()
        .as_str()
        .unwrap();

    let search_response = search_docs(
        email_index_reader_function_url,
        &access_key,
        &secret_key,
        SearchRequest {
            limit: Some(limit),
            query: Some(query.to_string()),
        },
    )
    .await?;

    let json = json!(&search_response);
    let pretty = serde_json::to_string_pretty(&json)?;

    info!("search response:\n{}", pretty);
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
