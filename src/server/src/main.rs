#[macro_use]
extern crate log;
use env_logger::Env;
extern crate clap;
use clap::{App, Arg};
#[macro_use]
extern crate serde;

use std::fs;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

use crate::server_state::ServerState;

mod commands;
mod conductor;
mod csv_utils;
mod database_state;
mod handler;
mod server_state;
mod sql_parser;

/// Re-export Storage manager here for this crate to use. This allows us to change
/// the storage manager by changing one use statement.
pub use memstore::storage_manager::StorageManager;

#[derive(Deserialize, Debug)]
struct ServerConfig {
    host: String,
    port: String,
    db_path: String,
    hf_path: String,
}

/// Entry point for server.
///
/// Waits for user connections and creates a new thread for each connection.
fn main() {
    // Configure log environment
    env_logger::from_env(Env::default().default_filter_or("debug")).init();

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .value_name("host")
                .default_value("127.0.0.1")
                .help("Server IP address")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("port")
                .default_value("3333")
                .help("Server port number")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db_path")
                .short("db")
                .long("db_path")
                .value_name("db_path")
                .default_value("persist/db/")
                .help("Path where DB is stored")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("hf_path")
                .long("hf_path")
                .value_name("hf_path")
                .default_value("persist/table/")
                .help("????")
                .takes_value(true),
        )
        .get_matches();

    let config = if let Some(c) = matches.value_of("config") {
        let config_path = c;
        let contents = fs::read_to_string(config_path).unwrap();
        serde_json::from_str(&contents).unwrap()
    } else {
        let host = matches.value_of("host").unwrap();
        let port = matches.value_of("port").unwrap();
        let db_path = matches.value_of("db_path").unwrap();
        let hf_path = matches.value_of("hf_path").unwrap();
        ServerConfig {
            host: host.to_string(),
            port: port.to_string(),
            db_path: db_path.to_string(),
            hf_path: hf_path.to_string(),
        }
    };

    info!("Starting crustydb... {:?}", config);

    let server_state = Arc::new(ServerState::new(config.db_path, config.hf_path).unwrap());

    let mut bind_addr = config.host.clone();
    bind_addr.push_str(":");
    bind_addr.push_str(&config.port);
    let listener = TcpListener::bind(bind_addr).unwrap();

    // Accept connections and process them on independent threads.
    info!(
        "Server listening on with host {} on port {}",
        config.host, config.port
    );
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                debug!("New connection: {}", stream.peer_addr().unwrap());
                let server_state = Arc::clone(&server_state);
                let _handler = thread::spawn(move || {
                    // Connection succeeded.
                    handler::handle_client_request(stream, server_state);
                });
            }
            Err(e) => {
                // Connection failed.
                error!("Error: {}", e);
            }
        }
    }
    // Close the socket server.
    drop(listener);
}
