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
use crate::s3_document_ingestor::S3DocumentIngestor;
use anyhow::Result;
use duckdb::arrow::{
    record_batch::RecordBatch,
    array::StringArray,
    datatypes::{Field, Schema},
};
use std::sync::Arc;
use futures::executor;
pub struct DocumentReader<'a> {
    ingestor: &'a S3DocumentIngestor
}

impl CkanReader for DocumentReader<'_> {
    fn supported_formats(&self) -> Vec<String> {
        vec!["CSV".to_string()]
    }

    fn do_read(&self, resource: &CkanResource) -> Result<Vec<RecordBatch>> {
        let download_url = executor::block_on(self.ingestor.ingest(
            [resource.id.as_str(), resource.format.to_lowercase().as_str()].join(".").as_str(),
            &resource.url,
            &resource.format
        ))?;

        // Criar o schema com uma coluna 'url'
        let schema = Arc::new(Schema::new(vec![
            Field::new("url", duckdb::arrow::datatypes::DataType::Utf8, false)
        ]));

        // Criar o array com o valor do download_url
        let url_array = StringArray::from(vec![download_url.as_str()]);

        // Criar o RecordBatch
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(url_array)]
        )?;

        Ok(vec![batch])
    }
}
