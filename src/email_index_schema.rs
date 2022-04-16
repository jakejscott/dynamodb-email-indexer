use anyhow::{Context, Result};
use log::info;
use std::{path::PathBuf, str::FromStr};
use tantivy::{
    schema::{Field, Schema, INDEXED, STORED, STRING, TEXT},
    Index,
};
pub struct EmailIndexSchema {
    pub schema: Schema,
    pub fields: EmailIndexFields,
}

pub struct EmailIndexFields {
    pub id: Field,
    pub timestamp: Field,
    pub subject: Field,
    pub body: Field,
    pub to: Field,
}

impl EmailIndexSchema {
    pub fn new() -> Self {
        let mut builder = Schema::builder();

        let id = builder.add_text_field("id", STRING | STORED);
        let timestamp = builder.add_i64_field("timestamp", INDEXED); // ADD FAST FIELD. Also test if indexed is needed
        let subject = builder.add_text_field("subject", TEXT);
        let body = builder.add_text_field("body", TEXT);
        let to = builder.add_text_field("to", TEXT);

        let schema = builder.build();

        let fields = EmailIndexFields {
            id,
            timestamp,
            to,
            body,
            subject,
        };

        EmailIndexSchema { schema, fields }
    }

    pub fn default_fields(&self) -> Vec<Field> {
        vec![
            self.fields.id,
            self.fields.timestamp,
            self.fields.subject,
            self.fields.body,
            self.fields.to,
        ]
    }

    pub fn ensure_index(&self) -> Result<Index> {
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

    fn open(&self) -> Result<Index> {
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
