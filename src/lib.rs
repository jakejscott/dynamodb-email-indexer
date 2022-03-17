use anyhow::{Context, Result};
use log::info;
use std::{path::PathBuf, str::FromStr};
use tantivy::{
    schema::{Field, Schema, STORED, STRING, TEXT},
    Index,
};

pub struct IndexSchema {
    pub schema: Schema,
    pub pk: Field,
    pub sk: Field,
    pub subject: Field,
    pub body: Field,
}

fn get_index_path() -> Result<PathBuf> {
    let mount_path = std::env::var("EFS_MOUNT_PATH").context("EFS_MOUNT_PATH env var missing")?;
    let path = PathBuf::from_str(mount_path.as_str()).context("EFS_MOUNT_PATH is not valid")?;
    let index_path = path.join(PathBuf::from("index"));
    Ok(index_path)
}

pub fn build_schema() -> IndexSchema {
    let mut builder = Schema::builder();

    let pk = builder.add_text_field("pk", STRING | STORED);
    let sk = builder.add_text_field("sk", STRING | STORED);
    let subject = builder.add_text_field("subject", TEXT);
    let body = builder.add_text_field("body", TEXT);

    let schema = builder.build();

    IndexSchema {
        schema,
        pk,
        sk,
        body,
        subject,
    }
}

pub fn ensure_index(index_schema: &IndexSchema) -> Result<Index> {
    let index_path = get_index_path()?;

    let index: Index;
    if !index_path.exists() {
        info!("creating index");
        std::fs::create_dir(&index_path).context("Error creating index dir")?;
        index = Index::create_in_dir(&index_path, index_schema.schema.clone())
            .context("Error creating index")?;
    } else {
        info!("opening index");
        index = open_index().context("Error opening index")?;
    }

    Ok(index)
}

pub fn open_index() -> Result<Index> {
    let index_path = get_index_path()?;
    let index = Index::open_in_dir(&index_path).context("Error opening index")?;
    Ok(index)
}
