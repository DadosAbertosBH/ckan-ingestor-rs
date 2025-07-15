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
use anyhow::Result;
use ckan_ingestor_rs::csv_reader::CsvReader;
use ckan_ingestor_rs::ckan_reader::CkanReader;
use ckan_ingestor_rs::ckan_resource::CkanResource;
use common::fixture_path;
use uuid::uuid;

#[test]
fn parse_latin_encoded_csv() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, ";".to_string());

    let resource = CkanResource{
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("csv_with_latin_encode.csv").to_str().unwrap().to_string(),
        format: "CSV".to_string(),
        datastore_active: false
    };

    let batches = reader.read(&resource)?;
    let total: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total, 2);
    Ok(())
}

#[test]
fn parse_non_latin_and_non_utf8() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, ";".to_string());

    let resource = CkanResource{
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("non_latin1_and_non_utf8.csv").to_str().unwrap().to_string(),
        format: "CSV".to_string(),
        datastore_active: false
    };

    let batches = reader.read(&resource)?;
    println!("{}" ,batches[0].num_rows());
    let total: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total, 2);
    Ok(())
}

#[test]
fn csv_with_bom() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, ",".to_string());

    let resource = CkanResource{
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("csv_with_bom.csv").to_str().unwrap().to_string(),
        format: "CSV".to_string(),
        datastore_active: false
    };
    let batches = reader.read(&resource)?;
    let total: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total, 804);
    Ok(())
}
