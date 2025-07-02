// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use duckdb::Connection;
use reqwest::blocking::Client;
use serde_json::Value;

const MAX_RECORDS_FETCH: usize = 100_000;

pub struct DatastoreReader {
    pub datastore_url: String,
    conn: Connection,
}

impl DatastoreReader {
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self {
            datastore_url: url.into(),
            conn,
        })
    }

    pub fn read(&self, resource_id: &str) -> Result<Vec<RecordBatch>> {
        let client = Client::new();
        let mut offset = 0;
        let mut batches = Vec::new();
        loop {
            let url = format!(
                "{}/{resource_id}?format=json&offset={offset}&limit={MAX_RECORDS_FETCH}",
                self.datastore_url
            );
            let resp = client.get(&url).send()?;
            let json: Value = resp.json()?;
            let recs = json
                .get("records")
                .ok_or_else(|| anyhow!("records missing"))?;
            let fields = json
                .get("fields")
                .ok_or_else(|| anyhow!("fields missing"))?;
            let recs = recs
                .as_array()
                .ok_or_else(|| anyhow!("records not array"))?;
            if recs.is_empty() {
                break;
            }
            let column_names: Vec<String> = fields
                .as_array()
                .ok_or_else(|| anyhow!("fields not array"))?
                .iter()
                .filter_map(|f| f.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .collect();
            let mut columns: Vec<Vec<String>> = vec![Vec::new(); column_names.len()];
            for rec in recs {
                let obj = rec
                    .as_object()
                    .ok_or_else(|| anyhow!("record not object"))?;
                for (i, name) in column_names.iter().enumerate() {
                    let val = obj
                        .get(name)
                        .map(|v| match v {
                            Value::String(s) => s.clone(),
                            Value::Null => String::new(),
                            _ => v.to_string(),
                        })
                        .unwrap_or_default();
                    columns[i].push(val);
                }
            }
            let arrays: Vec<ArrayRef> = columns
                .into_iter()
                .map(|c| Arc::new(StringArray::from(c)) as ArrayRef)
                .collect();
            let fields =
                column_names
                    .iter()
                    .map(|n| Field::new(n, DataType::Utf8, true))
                    .collect::<Vec<Field>>();
            let schema = Arc::new(Schema::new(fields));

            let batch = RecordBatch::try_new(schema, arrays)?;
            batches.push(batch);
            offset += MAX_RECORDS_FETCH;
        }
        if batches.is_empty() {
            Err(anyhow!("no rows"))
        } else {
            Ok(batches)
        }
    }
}
