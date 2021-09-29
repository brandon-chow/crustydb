use std::fs;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::StorageManager;
use common::catalog::Catalog;
use common::database::Database;
use common::ids::ContainerId;
use common::storage_trait::StorageTrait;
use common::table::Table;
use common::{get_attr, Attribute, CrustyError, QueryResult, TableSchema};
use sqlparser::ast::ColumnDef;

#[derive(Serialize)]
pub struct DatabaseState {
    pub id: u64,
    pub name: String,
    // pub catalog: Catalog,
    pub database: Database,

    #[serde(skip_serializing)]
    pub storage_manager: Arc<StorageManager>,

    // runtime information
    pub active_client_connections: RwLock<HashSet<u64>>,

    pub table_container_map: Arc<RwLock<HashMap<String, ContainerId>>>,
}

impl DatabaseState {
    // initializing within here
    pub fn new_from_path(path: PathBuf, storage_path: String) -> Result<Self, CrustyError> {
        debug!("Creating new DBState from path {:?}", path);
        // TODO: Remove magic numbers to parse out db json file name.
        let cand = path.display().to_string();
        // FIXME: that 11 hard-coded there....
        let cand_name = &cand[11..cand.len() - 5];
        debug!("cand: {} cand_name {}", cand, cand_name);

        match fs::File::open(cand.clone()) {
            Ok(res) => {
                let db_name = cand_name.to_string();
                let db_id = DatabaseState::get_database_id(db_name.clone());

                let storage_manager = Arc::new(StorageManager::new(storage_path));

                let database =
                    DatabaseState::load_database_from_file(res, &storage_manager).unwrap();

                let db_state = DatabaseState {
                    id: db_id,
                    name: db_name,
                    database,
                    storage_manager,
                    active_client_connections: RwLock::new(HashSet::new()),
                    table_container_map: Arc::new(RwLock::new(HashMap::new())),
                };
                Ok(db_state)
            }
            _ => return Err(CrustyError::IOError(String::from("Failed to open db file"))),
        }
    }

    pub fn get_database_id(db_name: String) -> u64 {
        let mut s = DefaultHasher::new();
        db_name.hash(&mut s);
        let db_id = s.finish();
        db_id
    }

    pub fn new_from_name(db_name: &str, storage_path: String) -> Result<Self, CrustyError> {
        let db_name: String = String::from(db_name);
        let db_id = DatabaseState::get_database_id(db_name.clone());
        debug!(
            "Creating new DatabaseState; name: {} id: {}",
            db_name, db_id
        );
        let database = Database::new(db_name.to_string());

        let storage_manager = Arc::new(StorageManager::new(storage_path));

        let db_state = DatabaseState {
            id: db_id,
            name: db_name,
            database,
            storage_manager,
            active_client_connections: RwLock::new(HashSet::new()),
            table_container_map: Arc::new(RwLock::new(HashMap::new())),
        };
        Ok(db_state)
    }

    pub fn register_new_client_connection(&self, client_id: u64) {
        debug!(
            "Registering new client connection: {:?} to database: {:?}",
            client_id, self.id
        );
        self.active_client_connections
            .write()
            .unwrap()
            .insert(client_id);
    }

    pub fn close_client_connection(&self, client_id: u64, metadata_path: String) {
        info!("Closing client connection: {:?}...", &client_id);
        // Remove client from this db
        self.active_client_connections
            .write()
            .unwrap()
            .remove(&client_id);
        // Check if that was the last client connected to this DB
        if self.active_client_connections.read().unwrap().is_empty() {
            // Construct path where db will be persisted
            let mut persist_path = metadata_path.clone();
            persist_path.push_str(&self.name);
            persist_path.push_str(".json");
            // Serialize DB into a string and write it to the path
            if let Ok(s) = serde_json::to_string(&self) {
                info!("Persisting db on: {:?}", &metadata_path);
                fs::write(&persist_path, s).expect("Failed to write out db json");
            }
        }
        info!("Closing client connection: {:?}...DONE", &client_id);
    }

    pub fn get_table_names(&self) -> Result<String, CrustyError> {
        let mut table_names = Vec::new();
        {
            let tables = self.database.get_tables();
            let tables_ref = tables.read().unwrap();
            for table in tables_ref.values() {
                let name = table.read().unwrap().name.clone();
                table_names.push(name);
            }
        }
        let table_names = table_names.join("\n");
        if table_names.is_empty() {
            Ok(String::from("No tables"))
        } else {
            Ok(table_names)
        }
    }

    /// Load in database.
    ///
    /// # Arguments
    ///
    /// * `db` - Name of database to load in.
    /// * `id` - Thread id to get the lock.
    pub fn load_database_from_file(
        file: fs::File,
        storage_manager: &StorageManager,
    ) -> Result<Database, CrustyError> {
        debug!("Loading DB from file {:?}", file);
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents)?;
        let db_content_str: &str = &contents;
        let db_cand: Database = serde_json::from_str(db_content_str).unwrap();
        {
            let mut tables_ref = db_cand.tables.write().unwrap();
            for table_ref in tables_ref.values_mut() {
                let table = table_ref.read().unwrap();

                debug!("Loading table: {:?}", table.name.clone());
                let table_id_downcast: u16 = table.id as u16;
                storage_manager.create_container(table_id_downcast).unwrap();
            }
        }
        Ok(db_cand)
    }

    /// Creates a new table.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the new table.
    /// * `cols` - Table columns.
    pub fn create_table(
        &self,
        table_name: &str,
        columns: &[ColumnDef],
    ) -> Result<QueryResult, CrustyError> {
        let db = &self.database;
        let mut tables_ref = db.tables.write().unwrap();
        let table_id = Table::get_table_id(table_name);
        if tables_ref.contains_key(&table_id) {
            return Err(CrustyError::CrustyError(String::from(
                "Table already exists ",
            )));
        }

        let mut attributes: Vec<Attribute> = Vec::new();
        for col in columns {
            let attr = Attribute {
                name: col.name.clone(),
                dtype: get_attr(&col.data_type)?,
            };
            attributes.push(attr);
        }
        let schema = TableSchema::new(attributes);
        debug!("Creating table with schema: {:?}", schema);

        let table = Table::new(table_name.to_string(), schema);
        let table_id_downcast = table.id as u16;
        &self.storage_manager.create_container(table_id_downcast);
        tables_ref.insert(table_id, Arc::new(RwLock::new(table)));
        Ok(QueryResult::new(&format!("Table {} created", table_name)))
    }
}
