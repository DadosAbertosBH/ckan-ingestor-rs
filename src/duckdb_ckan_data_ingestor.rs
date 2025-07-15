use crate::ckan_reader::CkanReader;
use crate::ckan_resource::CkanResource;
use anyhow::Result;
use arrow::array::RecordBatch;

pub struct DuckdbCkanDataIngestor<'a> {
    pub readers: &'a Vec<&'a dyn CkanReader>,
}

impl DuckdbCkanDataIngestor<'_> {
    pub fn ingest_ckan_data(&self, resource: &CkanResource) -> Result<()> {
        let data : &Vec<RecordBatch>;
        
        for reader  in self.readers {
            let result = reader.read(resource);
            if result.is_ok() {
                data = &result?;
                break;
            }
        }
        Ok(())
    }
}
