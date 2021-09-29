use crate::commands;
use crate::database_state::DatabaseState;
use crate::server_state::ServerState;
use crate::sql_parser::SQLParser;
use common::{get_name, CrustyError, QueryResult};
use optimizer::optimizer::Optimizer;
use queryexe::query::{Executor, TranslateAndValidate};
use sqlparser::ast::Statement;
use std::sync::Arc;
use txn_manager::transactions::Transaction;

pub struct Conductor {
    pub parser: SQLParser,
    pub optimizer: Optimizer,
    pub executor: Executor,
}

impl Conductor {
    pub fn new(
        parser: SQLParser,
        optimizer: Optimizer,
        executor: Executor,
    ) -> Result<Self, CrustyError> {
        let conductor = Conductor {
            parser,
            optimizer,
            executor,
        };
        Ok(conductor)
    }

    /// Processes command entered by the user.
    ///
    /// Only processes `Create`, `Connect`, `Import`, `ShowTables`, and `Reset` commands.
    ///
    /// # Arguments
    ///
    /// * `cmd` - Command to execute.
    /// * `id` - Thread id.
    pub fn run_command(
        &self,
        command: commands::Commands,
        client_id: u64,
        server_state: &Arc<ServerState>,
    ) -> Result<String, CrustyError> {
        match command {
            commands::Commands::Create(name) => {
                info!("Processing COMMAND::Create {:?}", name);
                server_state.create_database(name)
            }
            commands::Commands::Connect(name) => {
                // Check exists and load.
                // TODO: Figure out about using &str.
                info!("Processing COMMAND::Connect {:?}", name);
                server_state.connect_to_db(name, client_id)
            }
            commands::Commands::Import(path_and_name) => {
                info!("Processing COMMAND::Import {:?}", path_and_name);
                server_state.import_database(path_and_name, client_id)
            }
            commands::Commands::ShowTables => {
                info!("Processing COMMAND::ShowTables");
                let db_id_ref = server_state.active_connections.read().unwrap();
                match db_id_ref.get(&client_id) {
                    Some(db_id) => {
                        let db_ref = server_state.id_to_db.read().unwrap();
                        let db_state = db_ref.get(db_id).unwrap();

                        let table_names = db_state.get_table_names().unwrap();
                        Ok(table_names)
                    }
                    None => Ok(String::from("No active DB or DB not found")),
                }
            }
            commands::Commands::ShowDatabases => {
                info!("Processing COMMAND::ShowDatabases");
                let id_map = server_state.id_to_db.read();
                let mut names: Vec<String> = Vec::new();
                match id_map {
                    Ok(map) => {
                        for (id, db) in &*map {
                            debug!(" id {}", id);
                            names.push(db.name.clone());
                        }
                    }
                    _ => panic!("Failed to get lock"),
                }
                Ok(names.join(","))
            }
            commands::Commands::Reset => {
                info!("Processing COMMAND::Reset");
                let db_id_ref = server_state.active_connections.read().unwrap();
                match db_id_ref.get(&client_id) {
                    Some(db_id) => {
                        let db_ref = server_state.id_to_db.read().unwrap();
                        let db_state = db_ref.get(db_id).unwrap();
                        server_state.reset_database(&db_state.storage_manager)
                    }
                    None => Ok(String::from("No active DB or DB not found")),
                }
            }
        }
    }

    /// Runs SQL commands depending on the first statement.
    ///
    /// # Arguments
    ///
    /// * `cmd` - Tokenized command into statements.
    /// * `id` - Thread id for lock management.
    pub fn run_sql(
        &mut self,
        cmd: Vec<Statement>,
        db_state: &Arc<DatabaseState>,
    ) -> Result<common::QueryResult, CrustyError> {
        if cmd.is_empty() {
            Err(CrustyError::CrustyError(String::from("Empty SQL command")))
        } else {
            match cmd.first().unwrap() {
                Statement::CreateTable {
                    name: table_name,
                    columns,
                    constraints: _,  // ignoring
                    with_options: _, // ignoring
                    external: _,     // ignoring
                    file_format: _,  // ignoring
                    location: _,     // ignoring
                } => {
                    info!("Processing CREATE table: {:?}", table_name);
                    db_state.create_table(&get_name(&table_name)?, columns)
                }
                Statement::Query(qbox) => {
                    info!("Processing SQL Query");
                    self.run_query(qbox, &db_state)
                }
                _ => Err(CrustyError::CrustyError(String::from("Not supported "))),
            }
        }
    }

    /// Runs a given query.
    ///
    /// # Arguments
    ///
    /// * `query` - Query to run.
    /// * `id` - Thread id for lock management.
    fn run_query(
        &mut self,
        query: &sqlparser::ast::Query,
        db_state: &DatabaseState,
    ) -> Result<QueryResult, CrustyError> {
        let db = &db_state.database;
        // Parse query AST into a logical plan
        debug!("Obtaining Logical Plan from query's AST");
        let lp = TranslateAndValidate::from_sql(query, db)?;
        debug!("Optimizing logical plan...TODO");
        self.optimizer.do_your_work();

        // Start transaction
        let txn = Transaction::new();

        // After optimizer has done its job, we obtain a physical representation of this logical-plan
        // This physical representation depends on the Executor implementation, so Executors must
        // provide a function that takes a logical plan, catalog, storage manager, etc, and gives
        // back a physical plan which is a thing that the Executor knows how to interpret
        debug!("Configuring Storage Manager");
        &self.executor.configure_sm(&db_state.storage_manager);
        let physical_plan =
            Executor::logical_plan_to_op_iterator(&db_state.storage_manager, db, &lp, txn.tid())?;
        // We populate the executor with the state: physical plan, and storage manager ref
        debug!("Configuring Physical Plan");
        &self.executor.configure_query(physical_plan);

        // Finally, execute the query
        debug!("Executing query");
        let res = self.executor.execute();
        match res {
            Ok(qr) => Ok(qr),
            Err(e) => Err(e),
        }
    }
}
