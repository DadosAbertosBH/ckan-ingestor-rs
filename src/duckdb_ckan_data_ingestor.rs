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
use crate::s3_pdf_ingestor::S3DocumentIngestor;
use anyhow::Result;
use parking_lot::Mutex;
use serde_json::Value;
use std::fs::File;
use std::io::Write;

pub struct DuckdbCkanDataIngestor<'a> {
    pub lock: &'a Mutex<()>,
    pub document_ingestor: S3DocumentIngestor,
}

impl<'a> DuckdbCkanDataIngestor<'a> {
    pub fn ingest_ckan_data(&self, resource: &Value) -> Result<()> {
        let resource_id = resource
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let format = resource
            .get("format")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let url = resource.get("url").and_then(|v| v.as_str()).unwrap_or("");
        match format {
            "PDF" => {
                let _ = self.document_ingestor.ingest(
                    &format!("{resource_id}.pdf"),
                    url,
                    "application/pdf",
                )?;
            }
            _ => {
                let mut file = File::create(format!("{resource_id}.txt"))?;
                writeln!(file, "{url}")?;
            }
        }
        Ok(())
    }
}
