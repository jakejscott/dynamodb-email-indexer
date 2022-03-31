use anyhow::{Context, Result};
use aws_sdk_dynamodb::model::AttributeValue;
use log::info;
use serde::Serialize;
use std::{collections::HashMap, path::PathBuf, str::FromStr};
use tantivy::{
    schema::{Field, Schema, STORED, STRING, TEXT},
    Index,
};

#[derive(Serialize, Debug)]
pub struct Message {
    pub pk: String,
    pub sk: String,
    pub subject: String,
    pub body: String,
}

impl Message {
    pub fn attributes(self) -> HashMap<String, AttributeValue> {
        HashMap::from([
            ("PK".into(), AttributeValue::S(self.pk)),
            ("SK".into(), AttributeValue::S(self.sk)),
            ("subject".into(), AttributeValue::S(self.subject)),
            ("body".into(), AttributeValue::S(self.body)),
        ])
    }
}

pub struct IndexSchema {
    pub schema: Schema,
    pub pksk: Field,
    pub pk: Field,
    pub sk: Field,
    pub subject: Field,
    pub body: Field,
}

impl IndexSchema {
    pub fn new() -> Self {
        let mut builder = Schema::builder();

        let pksk = builder.add_text_field("pksk", STRING | STORED);
        let pk = builder.add_text_field("pk", STRING | STORED);
        let sk = builder.add_text_field("sk", STRING | STORED);

        let subject = builder.add_text_field("subject", TEXT);
        let body = builder.add_text_field("body", TEXT);

        let schema = builder.build();

        IndexSchema {
            schema,
            pksk,
            pk,
            sk,
            body,
            subject,
        }
    }

    pub fn create(&self) -> Result<Index> {
        let index_path = self.get_index_path()?;

        let index: Index;
        if !index_path.exists() {
            info!("creating index");
            std::fs::create_dir(&index_path).context("Error creating index dir")?;
            index = Index::create_in_dir(&index_path, self.schema.clone())
                .context("Error creating index")?;
        } else {
            info!("opening index");
            index = self.open().context("Error opening index")?;
        }

        Ok(index)
    }

    pub fn open(&self) -> Result<Index> {
        let index_path = self.get_index_path()?;
        let index = Index::open_in_dir(&index_path).context("Error opening index")?;

        Ok(index)
    }

    fn get_index_path(&self) -> Result<PathBuf> {
        let mount_path =
            std::env::var("EFS_MOUNT_PATH").context("EFS_MOUNT_PATH env var missing")?;

        let path = PathBuf::from_str(mount_path.as_str()).context("EFS_MOUNT_PATH is not valid")?;
        let index_path = path.join(PathBuf::from("index"));
        Ok(index_path)
    }
}
