use crate::attribute_helper::AttributeHelper;
use aws_sdk_dynamodb::model::AttributeValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct Email {
    pub id: String,
    pub timestamp: i64,
    pub subject: String,
    pub body: String,
    pub to: Vec<String>,
}

impl Email {
    pub fn attributes(self) -> HashMap<String, AttributeValue> {
        HashMap::from([
            ("id".into(), AttributeValue::S(self.id)),
            (
                "timestamp".into(),
                AttributeValue::S(self.timestamp.to_string()),
            ),
            ("subject".into(), AttributeValue::S(self.subject)),
            ("body".into(), AttributeValue::S(self.body)),
            ("to".into(), AttributeValue::Ss(self.to)),
        ])
    }

    pub fn from(attributes: &HashMap<String, AttributeValue>) -> anyhow::Result<Email> {
        let id = AttributeHelper::parse_string(attributes, "id")?;
        let timestamp = AttributeHelper::parse_int_64(attributes, "timestamp")?;
        let subject = AttributeHelper::parse_string(attributes, "subject")?;
        let body = AttributeHelper::parse_string(attributes, "body")?;
        let to = AttributeHelper::parse_string_array(attributes, "to")?;

        let email = Email {
            id: id,
            timestamp: timestamp,
            body: body,
            subject: subject,
            to: to,
        };

        Ok(email)
    }
}
