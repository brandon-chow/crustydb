use escargot::CargoBuild;
use std::io::{Read, Result, Write};
use std::net::{Shutdown, TcpStream};
use std::process::{Child, Stdio};

pub struct ServerWrapper {
    stream: TcpStream,
    child: Child,
}

impl ServerWrapper {
    fn setup_server() -> Result<Child> {
        CargoBuild::new()
            .bin("server")
            .current_release()
            .current_target()
            .manifest_path("../src/server/Cargo.toml")
            .run()
            .unwrap()
            .command()
            // .stderr(Stdio::null())
            // .stdout(Stdio::null())
            .spawn()
    }

    fn try_connect() -> Result<TcpStream> {
        let bind_addr = "127.0.0.1:3333".to_string();
        let stream = TcpStream::connect(bind_addr)?;
        stream.set_nodelay(true).unwrap();
        Ok(stream)
    }

    pub fn new() -> std::result::Result<ServerWrapper, String> {
        // Configure log environment
        let child = ServerWrapper::setup_server().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        match ServerWrapper::try_connect() {
            Ok(stream) => Ok(ServerWrapper { stream, child }),
            _ => Err("Failed to connect to server".to_owned()),
        }
    }

    pub fn close_client(&mut self) {
        println!("Sending close...");
        self.run_command_without_out("\\close");
        println!("Done...");
        self.stream
            .shutdown(Shutdown::Both)
            .expect("Shutdown occurred unsuccessfully");
        std::thread::sleep(std::time::Duration::from_millis(100));
        println!("About to kill client/server");
        self.child.kill().unwrap();
    }

    pub fn cleanup(&mut self) -> &mut Self {
        self.run_command("\\d")
    }

    pub fn run_command_without_out(&mut self, command: &str) {
        // Send command
        self.stream
            .write_all(format!("{}\n", command).as_bytes())
            .expect("Failed to write");
    }

    pub fn run_command_with_out(&mut self, command: &str) -> String {
        // Send command
        self.stream
            .write_all(format!("{}\n", command).as_bytes())
            .expect("Failed to write");
        // Read server response
        let mut data = [0 as u8; 256];
        while match self.stream.read(&mut data) {
            Ok(_size) => {
                //TODO: Remove echo and change to from_utf8
                // let s = String::from_utf8_lossy(&data);

                //TODO this is dirty. Should likely be response type sent to client.
                // //quit command received from server
                // if s.starts_with("\\") {
                //     if s.starts_with("\\quit") {
                //         info!("Received Quit Command");
                //         cont = false;
                //     } else {
                //         info!("command received {}", s);
                //         panic!("No action specified for command {}", s);
                //     }
                // }
                // info!("{}", s);
                false
            }
            Err(_) => false,
        } {}
        String::from_utf8(data.to_vec()).unwrap()

        // FIXME: this is a better way of reading the answer
        // println!("Command sent, waiting for response...");
        // let mut out = [0 as u8; 256];
        // self.stream.read_exact(&mut out).unwrap();
        // println!("response received!");
        // String::from_utf8(out.to_vec()).unwrap()
    }

    pub fn run_command(&mut self, command: &str) -> &mut Self {
        self.run_command_with_out(command);
        self
    }
}
