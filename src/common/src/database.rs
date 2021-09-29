use crate::catalog;
use crate::table::*;
use catalog::Catalog;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// The actual database.
#[derive(Clone, Serialize, Deserialize)]
pub struct Database {
    /// Name of the database.
    pub name: String,
    // Requires RwLock on both map and tables to enable adding/removing tables as well as table mutability.
    // TODO: can likely remove RwLock on table because all modifications to Table solely occur within the HeapFile.
    /// Locks for the tables.
    #[serde(skip)]
    pub tables: Arc<RwLock<HashMap<u64, Arc<RwLock<Table>>>>>,
}

impl Database {
    /// Initialize a new database with a given name.
    ///
    /// # Arguments
    ///
    /// * `name` - Name for the new database.
    pub fn new(name: String) -> Self {
        Database {
            name,
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Catalog for Database {
    /// Gets the tables from the catalog of the database.
    fn get_tables(&self) -> Arc<RwLock<HashMap<u64, Arc<RwLock<Table>>>>> {
        self.tables.clone()
    }
}

//TODO: Add catalog unit testing
