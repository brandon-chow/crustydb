use criterion::{black_box, criterion_group, Criterion};

use crate::benchmarks::benchtemplate::BenchTemplate;
use utilities::template::Template;

const BASE_PATH: &str =  "../test_data/";

fn bench_join_tiny(c: &mut Criterion) {
    let mut bt = Template::new();

    bt.setup.push(format!("CREATE TABLE testA (a INT, b INT);"));
    bt.setup.push(format!("\\i {}tiny_data.csv testA", BASE_PATH));

    bt.setup.push(format!("CREATE TABLE testB (a INT, b INT);"));
    bt.setup.push(format!("\\i {}tiny_data.csv testB", BASE_PATH));

    bt.add_command("select * from testA join testB on testA.a = testB.a");
    bt.show_configuration();
    bt.bench_server(c, "join_tiny");
}

fn bench_join_small(c: &mut Criterion) {
    let mut bt = Template::new();

    bt.setup.push(format!("CREATE TABLE testA (a INT, b INT);"));
    bt.setup.push(format!("\\i {}small_data.csv testA", BASE_PATH));

    bt.setup.push(format!("CREATE TABLE testB (a INT, b INT);"));
    bt.setup.push(format!("\\i {}small_data.csv testB", BASE_PATH));

    bt.add_command("select * from testA join testB on testA.a = testB.a");
    bt.show_configuration();
    bt.bench_server(c, "join_small");
}

fn bench_join_right(c: &mut Criterion) { 
    let mut bt = Template::new();

    bt.setup.push(format!("CREATE TABLE testA (a INT, b INT);"));
    bt.setup.push(format!("\\i {}right_data.csv testA", BASE_PATH));

    bt.setup.push(format!("CREATE TABLE testB (a INT, b INT);"));
    bt.setup.push(format!("\\i {}left_data.csv testB", BASE_PATH));

    bt.add_command("select * from testA join testB on testA.a = testB.a");
    bt.show_configuration();
    bt.bench_server(c, "join_right");
}

fn bench_join_left(c: &mut Criterion) {
    let mut bt = Template::new();

    bt.setup.push(format!("CREATE TABLE testA (a INT, b INT);"));
    bt.setup.push(format!("\\i {}right_data.csv testA", BASE_PATH));

    bt.setup.push(format!("CREATE TABLE testB (a INT, b INT);"));
    bt.setup.push(format!("\\i {}left_data.csv testB", BASE_PATH));

    bt.add_command("select * from testB join testA on testB.a = testA.a");
    bt.show_configuration();
    bt.bench_server(c, "join_left");
}

fn bench_join_large(c: &mut Criterion) {
    let mut bt = Template::new();

    bt.setup.push(format!("CREATE TABLE testA (a INT, b INT);"));
    bt.setup.push(format!("\\i {}large_data.csv testA", BASE_PATH));

    bt.setup.push(format!("CREATE TABLE testB (a INT, b INT);"));
    bt.setup.push(format!("\\i {}large_data.csv testB", BASE_PATH));

    bt.add_command("select * from testA join testB on testB.a = testA.a");
    bt.show_configuration();
    bt.bench_server(c, "join_large");
}

criterion_group! {
    name = joinbench;
    config = Criterion::default().sample_size(10);
    targets =
    bench_join_tiny,
    bench_join_small,
    bench_join_right,
    bench_join_left,
    bench_join_large,
}
