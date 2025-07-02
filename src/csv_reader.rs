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
use anyhow::Result;
use duckdb::{arrow::record_batch::RecordBatch, Connection};

pub struct CsvReader {
    conn: Connection,
}

impl CsvReader {
    pub fn new() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    pub fn read(&self, url: &str) -> Result<Vec<RecordBatch>> {
        let query = format!("SELECT * FROM read_csv_auto('{url}')");
        let mut stmt = self.conn.prepare(&query)?;
        let arrow_iterator = stmt.query_arrow([])?;
        let batches = arrow_iterator.collect();
        Ok(batches)
    }
}
