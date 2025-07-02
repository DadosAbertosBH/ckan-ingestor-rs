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
mod common;
use common::fixture_path;
use anyhow::Result;
use ckan_ingestor_rs::csv_reader::CsvReader;
use httpmock::Method::GET;
use httpmock::MockServer;

#[test]
fn parse_latin_encoded_csv() -> Result<()> {
    let reader = CsvReader::new()?;
    let path = fixture_path("csv_with_latin_encode.csv");
    let batches = reader.read(path.to_str().unwrap())?;
    assert!(!batches.is_empty());
    Ok(())
}

#[test]
fn parse_non_latin_and_non_utf8() -> Result<()> {
    let reader = CsvReader::new()?;
    let path = fixture_path("non_latin1_and_non_utf8.csv");
    let batches = reader.read(path.to_str().unwrap())?;
    assert!(!batches.is_empty());
    Ok(())
}

#[test]
fn csv_with_bom() -> Result<()> {
    let server = MockServer::start();
    let body = std::fs::read(fixture_path("csv_with_bom.csv"))?;
    server.mock(|when, then| {
        when.method(GET)
            .path("/datastore/csv_with_bom")
            .query_param("format", "csv");
        then.status(200)
            .header("Content-Type", "text/csv")
            .body(body.clone());
    });
    let reader = CsvReader::new()?;
    let url = format!(
        "http://{}/datastore/csv_with_bom?format=csv",
        server.address()
    );
    let batches = reader.read(&url)?;
    assert!(!batches.is_empty());
    Ok(())
}
