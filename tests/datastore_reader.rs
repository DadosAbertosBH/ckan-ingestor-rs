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
use ckan_ingestor_rs::datastore_reader::DatastoreReader;
use httpmock::{Method::GET, MockServer};

#[test]
fn read_datastore() -> Result<()> {
    let server = MockServer::start();
    let id = "a6b97d48-a9fb-4991-9893-d920ffb19b90";
    let body = std::fs::read_to_string(fixture_path(&format!("{id}.json")))?;
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "0")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body(body.clone());
    });
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "100000")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body("{\"fields\":[{\"id\":\"_id\"}],\"records\":[]}");
    });
    let reader = DatastoreReader::new(format!("http://{}", server.address()))?;
    let batches = reader.read(id)?;
    assert!(!batches.is_empty());
    Ok(())
}

#[test]
fn read_invalid_json() -> Result<()> {
    let server = MockServer::start();
    let id = "e3bce367-2e62-41c2-840f-b1df6255e7e5";
    let body = std::fs::read_to_string(fixture_path(&format!("{id}.json")))?;
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/datastore/{id}"))
            .query_param("format", "json")
            .query_param("offset", "0")
            .query_param("limit", "100000");
        then.status(200)
            .header("Content-Type", "application/json")
            .body(body.clone());
    });
    let reader = DatastoreReader::new(format!("http://{}", server.address()))?;
    let result = reader.read(id);
    assert!(result.is_err());
    Ok(())
}
