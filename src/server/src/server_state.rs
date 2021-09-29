use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};

use crate::csv_utils;
use crate::database_state::DatabaseState;
use common::table::Table;
use common::CrustyError;
use txn_manager::transactions::Transaction;

use crate::StorageManager;

pub struct ServerState {
    /// Path to database metadata files.
    pub storage_path: String,
    /// Path to heap files of the tables.
    pub metadata_path: String,

    // maps database id to DatabaseState
    pub id_to_db: RwLock<HashMap<u64, Arc<DatabaseState>>>,

    // runtime_information
    /// active connections indicates what client_id is connected to what db_id
    pub active_connections: RwLock<HashMap<u64, u64>>,
}

impl ServerState {
    // FIXME: probably will take a buffer pool configured outside, if any. Instead of
    // initializing within here
    pub fn new(metadata_path: String, storage_path: String) -> Result<Self, CrustyError> {
        // let meta_path = metadata_path.clone();
        // let stor_path = storage_path.clone();
        let server_state = ServerState {
            id_to_db: RwLock::new(HashMap::new()),
            active_connections: RwLock::new(HashMap::new()),
            /// Path to database metadata files.
            metadata_path,
            /// Path to heap files of the tables.
            storage_path,
        };

        // Create dirs if they do not exist.
        fs::create_dir_all(&server_state.storage_path)?;
        fs::create_dir_all(&server_state.metadata_path)?;

        // Create databases
        debug!("Looking for databases in {}", &server_state.storage_path);
        let paths = fs::read_dir(&server_state.storage_path).unwrap();
        {
            // for each path, create a DatabaseState
            for entry in paths {
                let path = entry.unwrap().path();
                debug!("Creating DatabaseState from path {:?}", path);
                let db_state = Arc::new(
                    DatabaseState::new_from_path(path, server_state.storage_path.clone()).unwrap(),
                );
                server_state
                    .id_to_db
                    .write()
                    .unwrap()
                    .insert(db_state.id, db_state);
            }
        }
        // TODO: does this pattern to make mutable things immutable make sense?
        let server_state = server_state;
        Ok(server_state)
    }

    fn get_db_id_from_db_name(&self, db_name: &str) -> Result<u64, CrustyError> {
        let map_ref = self.id_to_db.read().unwrap();
        for (db_id, db_state) in map_ref.iter() {
            if db_state.name == db_name {
                return Ok(db_id.clone());
            }
        }
        Err(CrustyError::CrustyError(String::from("db_name not found!")))
    }

    pub(crate) fn shutdown(&self) -> Result<(), CrustyError> {
        info!("Shutting down");
        Ok(())
    }

    /// Resets database to an empty database.
    pub fn reset_database(&self, _storage_manager: &StorageManager) -> Result<String, CrustyError> {
        // Clear data structures state
        info!("Resetting database... [To implement]");
        // self.id_to_db.write().unwrap().clear();
        // self.active_connections.write().unwrap().clear();
        // FIXME: uncomment when sm.reset is implemented
        // storage_manager.reset();

        // Clear storage.
        // fs::remove_dir_all(&self.metadata_path).unwrap();
        // fs::remove_dir_all(&self.storage_path).unwrap();
        // fs::create_dir_all(&self.metadata_path).unwrap();
        // fs::create_dir_all(&self.storage_path).unwrap();

        info!("Resetting database...DONE");
        Ok(String::from("Reset"))
    }

    pub fn close_client_connection(&self, client_id: u64) {
        // indicate DB this client is disconnecting
        let db_id_ref = self.active_connections.read().unwrap();
        match db_id_ref.get(&client_id) {
            Some(db_id) => {
                let db_ref = self.id_to_db.read().unwrap();
                let db = db_ref.get(db_id).unwrap();
                db.close_client_connection(client_id, self.metadata_path.clone());
            }
            None => {
                debug!("Client was not connected to DB");
            }
        };

        // remove this client from active connections
        self.active_connections.write().unwrap().remove(&client_id);
        info!(
            "Shutting down client connection with ID: {:?}...",
            client_id
        );
    }

    /// Creates a new database with name.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the new database.
    ///
    /// # Notes
    ///
    /// * The database is currently in-memory.
    pub fn create_database(&self, name: String) -> Result<String, CrustyError> {
        // Create new DB
        let db_state =
            Arc::new(DatabaseState::new_from_name(&name, self.storage_path.clone()).unwrap());
        // Represent newly created DB in server state
        self.id_to_db.write().unwrap().insert(db_state.id, db_state);
        Ok(format!("Created database {:?}", &name))
    }

    pub fn connect_to_db(&self, db_name: String, client_id: u64) -> Result<String, CrustyError> {
        let db_id = self.get_db_id_from_db_name(&db_name)?;
        let map_ref = self.id_to_db.read().unwrap();
        let db_state = map_ref.get(&db_id).unwrap();
        {
            let mut reference = self.active_connections.write().unwrap();
            reference.insert(client_id, db_state.id);
        }
        db_state.register_new_client_connection(client_id);
        Ok(format!("Connected to database {:?}", &db_name))
    }

    /// Import database from csv file at path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path of the csv file containing database.
    pub fn import_database(&self, path: String, client_id: u64) -> Result<String, CrustyError> {
        // TODO: Fix serialization.
        let mut flag = false;

        // TODO: Use itertools to clean this up?
        let mut new_path = "";
        let mut table_name = "";
        for token in path.split_whitespace() {
            if flag {
                table_name = token;
            } else {
                new_path = token;
                flag = true;
            }
        }

        let txn = Transaction::new();

        let db_id_ref = self.active_connections.read().unwrap();
        let db_id = db_id_ref.get(&client_id).unwrap();
        let db_state_ref = self.id_to_db.read().unwrap();
        let db_state = db_state_ref.get(db_id).unwrap();
        let db = &db_state.database;
        let tables = db.tables.read().unwrap();
        let table_id = Table::get_table_id(table_name);

        // Check if table name exists in active database.
        if let Some(table) = tables.get(&table_id) {
            let table_ref = &table.read().unwrap();
            // FIXME: Error check on import_csv.
            let _ = csv_utils::import_csv(
                table_ref,
                new_path.to_string(),
                txn.tid(),
                &db_state.storage_manager,
            )?;
            Ok(format!(
                "Data from path: {:?} imported to table: {:?}",
                &path,
                table_name.clone()
            ))
        } else {
            Err(CrustyError::CrustyError(String::from(
                "Table does not exist",
            )))
        }
    }
}
