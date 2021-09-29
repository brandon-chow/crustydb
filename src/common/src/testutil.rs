use crate::{Attribute, DataType, Field, TableSchema, Tuple};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::env;
use std::path::PathBuf;

pub fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
}

/// Converts an int vector to a Tuple.
///
/// # Argument
///
/// * `data` - Data to put into tuple.
pub fn int_vec_to_tuple(data: Vec<i32>) -> Tuple {
    let mut tuple_data = Vec::new();

    for val in data {
        tuple_data.push(Field::IntField(val));
    }

    Tuple::new(tuple_data)
}

/// Creates a Vec of tuples containing IntFields given a 2D Vec of i32 's
pub fn create_tuple_list(tuple_data: Vec<Vec<i32>>) -> Vec<Tuple> {
    let mut tuples = Vec::new();
    for item in &tuple_data {
        let fields = item.iter().map(|i| Field::IntField(*i)).collect();
        tuples.push(Tuple::new(fields));
    }
    tuples
}

/// Creates a new table schema for a table with width number of IntFields.
pub fn get_int_table_schema(width: usize) -> TableSchema {
    let mut attrs = Vec::new();
    for _ in 0..width {
        attrs.push(Attribute::new(String::new(), DataType::Int))
    }
    TableSchema::new(attrs)
}

pub fn get_random_byte_vec(n: usize) -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..n).map(|_| rand::random::<u8>()).collect();
    random_bytes
}

pub fn gen_rand_string(n: usize) -> String {
    thread_rng().sample_iter(Alphanumeric).take(n).map(char::from).collect()
}

pub fn gen_random_dir() -> PathBuf {
    init();
    let mut dir = env::temp_dir();
    dir.push(String::from("crusty"));
    let rand_string = gen_rand_string(10);
    dir.push(rand_string);
    dir
}

pub fn get_random_vec_of_byte_vec(n: usize, min_size: usize, max_size: usize) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    for _ in 0..n {
        res.push((min_size..max_size).map(|_| rand::random::<u8>()).collect());
    }
    res
}


pub fn compare_unordered_byte_vecs(a: &Vec<Vec<u8>>, mut b: Vec<Vec<u8>>) -> bool {
    // Quick check
    if a.len() != b.len() {
        return false;
    }
    // check if they are the same ordered
    let non_match_count = a.iter().zip(b.iter()).filter(|&(j,k)| j[..] != k[..]).count();
    if non_match_count == 0 {
        return true;
    }

    // Now check if they are out of order
    for x in a {
        let pos = b.iter().position(|y| y[..] == x[..]);
        match pos {
            None => {
                //Was not found, not equal
                return false;
            },
            Some(idx) => {
                b.swap_remove(idx);
            }
        }
    }
    //since they are the same size, b should be empty
    if !b.is_empty() {
        false
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    #[test]
    fn test_compare() {
        let mut rng = thread_rng();
        let a = get_random_vec_of_byte_vec(100, 10, 20);
        let b = a.clone();
        assert_eq!(true, compare_unordered_byte_vecs(&a, b));
        let mut b = a.clone();
        b.shuffle(&mut rng);
        assert_eq!(true, compare_unordered_byte_vecs(&a, b));
        let new_rand = get_random_vec_of_byte_vec(99,10, 20);
        assert_eq!(false, compare_unordered_byte_vecs(&a, new_rand));
        let mut b = a.clone();
        b[rng.gen_range(0..a.len())] = get_random_byte_vec(10);
        assert_eq!(false, compare_unordered_byte_vecs(&a, b));
    }
    
}
