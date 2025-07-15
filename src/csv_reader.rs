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
use crate::ckan_reader::CkanReader;
use crate::ckan_resource::CkanResource;
use anyhow::Result;
use duckdb::{arrow::record_batch::RecordBatch, Connection};

pub struct CsvReader<'a> {
    conn: &'a Connection,
    delim: String,
}

impl<'a> CsvReader<'a> {
    pub fn new(conn: &'a Connection, delim: String) -> Self {
        Self { conn, delim }
    }
}

impl CkanReader for CsvReader<'_> {
    fn supported_formats(&self) -> Vec<String> {
        vec!["CSV".to_string()]
    }

    fn do_read(&self, resource: &CkanResource) -> Result<Vec<RecordBatch>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM read_csv_auto(?, delim=?)")?;
        let arrow_iterator = stmt.query_arrow([resource.url.clone(), self.delim.clone()])?;
        let batches = arrow_iterator.collect();
        Ok(batches)
    }
}
