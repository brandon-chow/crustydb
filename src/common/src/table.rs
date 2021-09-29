use crate::TableSchema;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Table implementation.
#[derive(Serialize, Deserialize, Clone)]
pub struct Table {
    /// Table name.
    pub name: String,
    /// Table id.
    pub id: u64,
    /// Table schema.
    pub schema: TableSchema,
}

impl Table {
    // TODO: Replace hash of name with hash of absolute file path?
    /// Creates a new table with the given name and heapfile.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of table.
    /// * `file` - HeapFile of the table.
    pub fn new(name: String, schema: TableSchema) -> Self {
        let table_id = Table::get_table_id(&name);

        Table {
            name,
            id: table_id,
            schema,
        }
    }

    /// Creates table id of the table by hashing the table name.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of table to get the id for.
    pub fn get_table_id(name: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        hasher.finish()
    }
}
