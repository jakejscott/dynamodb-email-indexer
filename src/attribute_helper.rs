use aws_sdk_dynamodb::model::AttributeValue;
use std::collections::HashMap;

pub struct AttributeHelper;

impl AttributeHelper {
    pub fn parse_string(
        attributes: &HashMap<String, AttributeValue>,
        attribute_name: &str,
    ) -> anyhow::Result<String> {
        if let Some(attr) = attributes.get(attribute_name) {
            if let AttributeValue::S(value) = attr {
                return Ok(value.clone());
            }
        }

        Err(anyhow::anyhow!("{attribute_name} missing"))
    }

    pub fn parse_int_64(
        attributes: &HashMap<String, AttributeValue>,
        attribute_name: &str,
    ) -> anyhow::Result<i64> {
        if let Some(attr) = attributes.get(attribute_name) {
            if let AttributeValue::S(value) = attr {
                let result: i64 = value.parse()?;
                return Ok(result);
            }
        }

        Err(anyhow::anyhow!("{attribute_name} missing"))
    }

    pub fn parse_string_array(
        attributes: &HashMap<String, AttributeValue>,
        attribute_name: &str,
    ) -> anyhow::Result<Vec<String>> {
        if let Some(attr) = attributes.get(attribute_name) {
            if let AttributeValue::Ss(values) = attr {
                return Ok(values.clone());
            }
        };

        Err(anyhow::anyhow!("{attribute_name} missing"))
    }
}
