extern crate sqlparser;
use sqlparser::parser::*;

use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{Shutdown, TcpStream};

use crate::conductor::Conductor;
use crate::server_state::ServerState;

use crate::commands;
use crate::sql_parser::SQLParser;
use optimizer::optimizer::Optimizer;
use queryexe::query::Executor;
use sqlparser::ast::Statement;
use sqlparser::parser::ParserError;

pub enum Request {
    Err,
    Command(commands::Commands),
    SQLError(ParserError),
    SQL(Vec<Statement>),
}

/// Separates user input requests into commands and SQL inputs.
///
/// # Arguments
///
/// * `cmd` - String containing user's input.
fn parse_input_request(cmd: String) -> Request {
    let dialect = sqlparser::dialect::GenericDialect {};
    if cmd.starts_with('\\') {
        match commands::parse_command(cmd) {
            Some(c) => Request::Command(c),
            None => Request::Err,
        }
    } else {
        match Parser::parse_sql(&dialect, cmd) {
            Ok(a) => Request::SQL(a),
            Err(e) => Request::SQLError(e),
        }
    }
}

/// Waits for user commands and dispatches the commands.
///
/// # Arguments
///
/// * `stream` - TCP stream containing user inputs.
pub fn handle_client_request(mut stream: TcpStream, server_state: Arc<ServerState>) {
    let mut data = String::new();
    let mut buf_stream = BufReader::new(stream.try_clone().expect("Failed to clone stream"));

    // FIXME: right now, this is unused
    let parser = SQLParser::new();
    let executor = Executor::new_ref();
    let optimizer = Optimizer::new();
    let mut conductor = Conductor::new(parser, optimizer, executor).unwrap();

    // FIXME: id is hash(incoming-ip), make this right
    // TODO: create a session for this client
    let peer_ip_string = stream.peer_addr().unwrap().ip().to_string();
    let mut s = DefaultHasher::new();
    peer_ip_string.hash(&mut s);
    let client_id = s.finish();

    let mut quiet = false;
    while match buf_stream.read_line(&mut data) {
        Ok(size) => {
            debug!("{}", data);
            //TODO: Better way to handle client end?
            // FIXME: and close connection should be just another command
            if size == 0 || data == "\\close\n" {
                server_state.close_client_connection(client_id);
                false
            } else if data == "\\shutdown\n" {
                let quit = String::from("\\quit");
                stream.write_all(quit.as_bytes()).unwrap();
                data.clear();
                stream.shutdown(Shutdown::Both).unwrap();
                server_state.shutdown().unwrap();
                std::process::exit(1);
            } else if data == "\\quiet\n" {
                quiet = true;
                stream.write_all("QUIET MODE".to_string().as_bytes()).unwrap();
                true
            } else {
                let line = match String::from_utf8(data.as_bytes()[0..size].to_vec()) {
                    Ok(s) => s,
                    _ => return,
                };

                let response: String = match parse_input_request(line.to_string()) {
                    // COMMAND
                    Request::Command(a) => match conductor.run_command(a, client_id, &server_state)
                    {
                        Ok(qr) => {
                            info!("Success COMMAND::Create {:?}", qr);
                            qr.to_string()
                        }
                        Err(err) => {
                            info!("Error while executing COMMAND::Create; error: {:?}", err);
                            err.to_string()
                        }
                    },
                    // SQL Query
                    Request::SQL(ast) => {
                        let db_state = {
                            let db_id_ref = server_state.active_connections.read().unwrap();
                            let db_id = db_id_ref.get(&client_id).unwrap();
                            let db_ref = server_state.id_to_db.read().unwrap();
                            db_ref.get(db_id).unwrap().clone()
                        };
                        match conductor.run_sql(ast, &db_state) {
                            Ok(qr) => {
                                info!("Success running SQL query");
                                qr.result().to_string()
                            }
                            Err(err) => {
                                info!("Error while executing SQL query");
                                err.to_string()
                            }
                        }
                    }
                    // Errors
                    Request::SQLError(e) => format!("SQL error: {}", e),
                    Request::Err => "Unknown command".to_string(),
                };
                if quiet {
                     stream.write_all("ok".to_string().as_bytes()).unwrap();
                 } else {
                     stream.write_all(response.as_bytes()).unwrap();
                 }
                data.clear();
                true
            }
        }
        Err(_) => {
            error!(
                "An error occurred, terminating connection with {}",
                stream.peer_addr().unwrap()
            );
            stream.shutdown(Shutdown::Both).unwrap();
            // FIXME: (raul) shut this down properly
            error!("Shutting down crustydbd due to error...");
            std::process::exit(0);
        }
    } {}
}
