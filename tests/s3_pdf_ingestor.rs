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
use ckan_ingestor_rs::config::S3Settings;
use ckan_ingestor_rs::s3_pdf_ingestor::S3DocumentIngestor;
use httpmock::{Method::GET, MockServer};

#[test]
fn ingest_pdf() -> Result<()> {
    let server = MockServer::start();
    let body = std::fs::read(fixture_path("a_pdf_file.pdf"))?;
    server.mock(|when, then| {
        when.method(GET)
            .path("/datastore/a_pdf_file")
            .query_param("format", "PDF");
        then.status(200)
            .header("Content-Type", "application/pdf")
            .body(body.clone());
    });
    let settings = S3Settings {
        endpoint: server.address().to_string(),
        bucket: "warehouse".into(),
        use_ssl: false,
        ..Default::default()
    };
    let ingestor = S3DocumentIngestor::new(settings)?;
    let url = format!(
        "http://{}/datastore/a_pdf_file?format=PDF",
        server.address()
    );
    let returned = ingestor.ingest("test.pdf", &url, "application/pdf")?;
    let path = ingestor.base_path.join("test.pdf");
    assert!(path.exists());
    assert!(returned.ends_with("/test.pdf"));
    std::fs::remove_file(&path)?;
    std::fs::remove_dir_all(&ingestor.base_path)?;
    Ok(())
}
