use common::ids::{ContainerId, TransactionId};
use common::storage_trait::StorageTrait;
use common::table::Table;
use common::{CrustyError, DataType, Field, Tuple};
use std::fs::File;

use memstore::storage_manager::StorageManager;

/// Function to import csv data into an existing table within a database.
///
/// Note: This function does not perform any verification on column typing.
///
/// # Arguments
///
/// * `table` - Pointer to table to store the data in.
/// * `path` - Path to the csv file.
/// * `tid` - Transaction id for inserting the tuples.
pub fn import_csv(
    table: &Table,
    path: String,
    tid: TransactionId,
    storage_manager: &StorageManager,
) -> Result<(), CrustyError> {
    debug!("server::csv_utils trying to open file, path: {:?}", path);
    let file = File::open(path)?;
    // Create csv reader.
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file);

    //get storage container
    let table_id_downcast = table.id as u16;
    let container_id = table_id_downcast as ContainerId;
    storage_manager.create_container(table_id_downcast).unwrap();
    // Iterate through csv records.
    let mut inserted_records = 0;
    for result in rdr.records() {
        #[allow(clippy::single_match)]
        match result {
            Ok(rec) => {
                // Build tuple and infer types from schema.
                let mut tuple = Tuple::new(Vec::new());
                for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                    // TODO: Type mismatch between attributes and record data>
                    match &attr.dtype() {
                        DataType::Int => {
                            let value: i32 = field.parse::<i32>().unwrap();
                            tuple.field_vals.push(Field::IntField(value));
                        }
                        DataType::String => {
                            let value: String = field.to_string().clone();
                            tuple.field_vals.push(Field::StringField(value));
                        }
                    }
                }
                //TODO: How should individual row insertion errors be handled?
                debug!("server::csv_utils about to insert tuple into container_id: {:?}", &container_id);
                storage_manager.insert_value(container_id, tuple.get_bytes(), tid);
                inserted_records += 1;
            }
            _ => {
                // FIXME: get error from csv reader
                error!("Could not read row from CSV");
            }
        }
    }
    info!("Num records imported: {:?}", inserted_records);
    Ok(())
}
