extern crate clap;
extern crate rustyline;
use clap::{App, Arg};
use env_logger::Env;
use log::{error, info};
use serde::Deserialize;

use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

#[derive(Deserialize, Debug)]
struct ClientConfig {
    host: String,
    port: String,
}

fn process_input(stream: &mut TcpStream, line: &str) -> bool {
    stream.write_all(format!("{}\n", line).as_bytes()).unwrap();

    let mut data = [0 as u8; 256];
    loop {
        match stream.read(&mut data) {
            Ok(_size) => {
                //TODO: Remove echo and change to from_utf8
                let s = String::from_utf8_lossy(&data);

                //TODO this is dirty. Should likely be response type sent to client.
                //quit command received from server
                if s.starts_with("\\") {
                    if s.starts_with("\\quit") {
                        info!("Received Quit Command");
                        return false;
                    } else {
                        info!("command received {}", s);
                        panic!("No action specified for command {}", s);
                    }
                }
                info!("{}", s);
                return true;
            },
            Err(_) => return true
        }
    };
}

#[allow(unused_must_use)]
fn process_cli_input(stream: &mut TcpStream) {
    let mut rl = Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        info!("No previous history.");
    }
    let prompt: &str = "[crustydb]>>";
    let mut cont = true;
    while cont {
        let readline = rl.readline(prompt);
        match readline {
            Ok(line) => {
                if line.as_str() == "" {
                    continue;
                }
                rl.add_history_entry(line.as_str());
                cont = process_input(stream, line.as_str());
            }
            Err(ReadlineError::Interrupted) => {
                info!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                info!("CTRL-D");
                break;
            }
            Err(err) => {
                error!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt").unwrap();

    //TODO: error handle on shutdown.
    stream.shutdown(Shutdown::Both);
}

#[allow(unused_must_use)]
fn process_script_input(stream: &mut TcpStream, script: String) {
    let lines = script.split(";");
    for line in lines {
        let command = line.trim();
        if command == "" {
            continue;
        } 
        let clean_command = &command.replace("\n", " ");
        info!("Script clean command: {}", clean_command);

        if !process_input(stream, clean_command) {
            panic!("Bad Script");
        }
    }

    //TODO: error handle on shutdown.
    stream.shutdown(Shutdown::Both);
}

fn main() {
    // Configure log environment
    env_logger::from_env(Env::default().default_filter_or("info")).init();

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
                .default_value("0.0.0.0")
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
            Arg::with_name("script")
            .short("s")
            .long("script")
            .value_name("CRUSTY_SCRIPT")
            .help("Takes in a semicolon delimited file of crusty commands and SQL queries.")
            .takes_value(true)
            .required(false),
        )
        .get_matches();

    let config = if let Some(c) = matches.value_of("config") {
        let config_path = c;
        let contents = fs::read_to_string(config_path).unwrap();
        serde_json::from_str(&contents).unwrap()
    } else {
        let host = matches.value_of("host").unwrap();
        let port = matches.value_of("port").unwrap();
        ClientConfig {
            host: host.to_string(),
            port: port.to_string(),
        }
    };

    info!("Starting client with config: {:?}", config);

    let script:String = if let Some(s) = matches.value_of("script") {
        let script_path = s;
        fs::read_to_string(script_path).unwrap()
    } else {
        String::new()
    };

    let mut bind_addr = config.host.clone();
    bind_addr.push_str(":");
    bind_addr.push_str(&config.port);

    match TcpStream::connect(bind_addr) {
        Ok(mut stream) => {
            if script.is_empty() {
                process_cli_input(&mut stream);
            } else {
                process_script_input(&mut stream, script);
            }
        },
        Err(e) => {
            error!("Failed to connect: {}", e);
        }
    }
    info!("Terminated.");
}
