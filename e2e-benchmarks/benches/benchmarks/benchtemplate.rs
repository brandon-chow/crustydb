use utilities::template::Template;

use criterion::{black_box, Criterion};
use std::time::{Duration, Instant};

pub trait BenchTemplate {
    fn bench_server(&mut self, c: &mut Criterion, name: &str);
}

impl BenchTemplate for Template {
    fn bench_server(&mut self, c: &mut Criterion, name: &str) {
        // println!("Running setup for {:?}", name);
        self.run_setup();
        // println!("Starting main benchmark function...");
        c.bench_function(name, |b| b.iter(|| self.run_commands()));
        // println!("Starting main benchmark function...END");
        // println!("Cleaning up {:?}", name);
        self.run_cleanup();
    }
}
