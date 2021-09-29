use crate::serverwrapper::ServerWrapper;

use rand::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use common::{Field, Tuple};

pub struct Template {
    pub setup: Vec<String>,
    commands: Vec<String>,
    cleanup: Vec<String>,
    server: ServerWrapper,
}

impl Default for Template {
    fn default() -> Self {
        Self::new()
    }
}

impl Template {
    pub fn new() -> Template {
        Template {
            setup: vec!["\\quiet\n".to_owned(), "\\r db".to_owned(), "\\c db".to_owned()],
            commands: Vec::new(),
            cleanup: Vec::new(),
            server: ServerWrapper::new().unwrap(),
        }
    }

    pub fn show_configuration(&self) {
        println!("setup: {:?}", &self.setup);
        println!("commands: {:?}", &self.commands);
        println!("cleanup: {:?}", &self.cleanup);
    }

    fn create_import_file(&self, name: String, tuples: &[Tuple]) {
        let mut res = String::new();
        for tup in tuples.iter() {
            for field in tup.field_vals() {
                let val = match field {
                    Field::IntField(i) => i.to_string(),
                    Field::StringField(s) => s.to_string(),
                };
                res.push_str(&val);
                res.push_str(",");
            }
            res.push_str("\n");
        }

        let mut file = File::create(Path::new((name + ".txt").as_str())).unwrap();
        file.write_all(res.as_bytes()).unwrap();
    }

    pub fn generate_random_table(&mut self, name: &str, columns: i32, rows: i32) -> Vec<Tuple> {
        let mut rng = rand::thread_rng();

        let mut tuples: Vec<Tuple> = Vec::new();
        for _ in 0..rows {
            let mut fields: Vec<Field> = Vec::new();
            for _ in 0..columns {
                fields.push(Field::IntField(rng.gen_range(0, i32::MAX)));
            }
            tuples.push(Tuple::new(fields));
        }
        self.push_table(name, columns, &tuples);

        tuples
    }

    pub fn push_table(&mut self, name: &str, columns: i32, tuples: &[Tuple]) {
        let mut fs = "(".to_owned();
        for i in 0..columns {
            fs.push_str(&format!("f{} int,", i));
        }
        fs.pop();
        fs.push(')');

        self.create_import_file(name.to_owned(), &tuples);

        self.setup.push(format!("create table {} {}", name, fs));
        self.setup.push(format!("\\i ../{}.txt {}", name, name));
    }

    pub fn add_command(&mut self, cmd: &str) {
        self.commands.push(cmd.to_owned());
    }

    pub fn run_setup(&mut self) {
        for command in self.setup.iter() {
            // println!("Benchmark -- Running command: {:?}", command);
            self.server.run_command(command);
        }
    }

    pub fn run_command_with_out(&mut self, command: &str) -> String {
        self.server.run_command_with_out(command)
    }

    pub fn run_commands(&mut self) {
        // println!("# commands to run: {:?}", self.commands);
        for command in self.commands.iter() {
            // println!("Running command: {:?}", command);
            self.server.run_command(command);
        }
    }

    pub fn run_cleanup(&mut self) {
        // println!("rust_cleanup...");
        for command in self.cleanup.iter() {
            // println!("Running command: {:?}", command);
            self.server.run_command(command);
        }
        self.server.cleanup();
        self.server.close_client();
        // println!("rust_cleanup...OK");
    }

    pub fn reset(&mut self) -> &mut Self {
        self.server.run_command("\\d");
        self.run_setup();
        self
    }
}
