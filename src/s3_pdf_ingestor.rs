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
use crate::config::S3Settings;
use anyhow::Result;
use reqwest::blocking::Client;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

pub struct S3DocumentIngestor {
    pub base_path: PathBuf,
    pub public_url: String,
}

impl S3DocumentIngestor {
    pub fn new(settings: S3Settings) -> Result<Self> {
        let base_path = PathBuf::from("docs");
        std::fs::create_dir_all(&base_path)?;
        let protocol = if settings.use_ssl { "https" } else { "http" };
        let public_url = format!("{}://{}/{}", protocol, settings.endpoint, settings.bucket);
        Ok(Self {
            base_path,
            public_url,
        })
    }

    pub fn ingest(
        &self,
        filename: &str,
        download_url: &str,
        _content_type: &str,
    ) -> Result<String> {
        let client = Client::new();
        let resp = client.get(download_url).send()?;
        let bytes = resp.bytes()?;
        let path = self.base_path.join(filename);
        let mut file = File::create(&path)?;
        file.write_all(&bytes)?;
        Ok(format!("{}/{}", self.public_url, filename))
    }
}
