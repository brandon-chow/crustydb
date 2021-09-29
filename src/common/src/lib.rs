extern crate csv;
#[macro_use]
extern crate serde;
extern crate log;

use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};
use sqlparser::ast;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io;
pub mod crusty_graph;
pub mod logical_plan;
pub use logical_plan::{AggOp, PredicateOp};
pub mod catalog;
pub mod database;
pub mod ids;
pub mod storage_trait;
pub mod table;
pub mod testutil;

/// How big each page is
pub const PAGE_SIZE: usize = 4096;
// How many pages a buffer pool can hold
pub const PAGE_SLOTS: usize = 50;

/// Custom error type.
#[derive(Debug, Clone, PartialEq)]
pub enum CrustyError {
    /// IO Errors.
    IOError(String),
    /// Custom errors.
    CrustyError(String),
    /// Validation errors.
    ValidationError(String),
    /// Execution errors.
    ExecutionError(String),
    /// Transaction aborted.
    TransactionAbortedError,
}

impl fmt::Display for CrustyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                CrustyError::ValidationError(s) => format!("Validation Error: {}", s),
                CrustyError::ExecutionError(s) => format!("Execution Error: {}", s),
                CrustyError::CrustyError(s) => format!("Crusty Error: {}", s),
                CrustyError::IOError(s) => s.to_string(),
                CrustyError::TransactionAbortedError => String::from("Transaction Aborted Error"),
            }
        )
    }
}

// Implement std::convert::From for AppError; from io::Error
impl From<io::Error> for CrustyError {
    fn from(error: io::Error) -> Self {
        CrustyError::IOError(error.to_string())
    }
}

impl Error for CrustyError {}

/// Return type for a query result.
pub struct QueryResult {
    result: String,
}

impl QueryResult {
    /// Return an empty result.
    pub fn empty() -> Self {
        Self {
            result: String::from(""),
        }
    }

    /// Return a result with string.
    ///
    /// # Arguments
    ///
    /// * `result` - Result to return.
    pub fn new(result: &str) -> Self {
        Self {
            result: result.to_string(),
        }
    }

    /// Get the result.
    pub fn result(&self) -> &str {
        &self.result
    }
}

/// Handle schemas.
#[derive(PartialEq, Clone, Debug)]
pub struct TableSchema {
    /// Attributes of the schema.
    attributes: Vec<Attribute>,
    /// Mapping from attribute name to order in the schema.
    name_map: HashMap<String, usize>,
}

impl Serialize for TableSchema {
    /// Custom serialize to avoid serializing name_map.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.attributes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TableSchema {
    /// Custom deserialize to avoid serializing name_map.
    fn deserialize<D>(deserializer: D) -> Result<TableSchema, D::Error>
    where
        D: Deserializer<'de>,
    {
        let attrs = Vec::deserialize(deserializer)?;
        Ok(TableSchema::new(attrs))
    }
}

impl TableSchema {
    /// Create a new schema.
    ///
    /// # Arguments
    ///
    /// * `attributes` - Attributes of the schema in the order that they are in the schema.
    pub fn new(attributes: Vec<Attribute>) -> Self {
        let mut name_map = HashMap::new();
        for (i, attr) in attributes.iter().enumerate() {
            name_map.insert(attr.name().to_string(), i);
        }
        Self {
            attributes,
            name_map,
        }
    }

    /// Create a new schema with the given names and dtypes.
    ///
    /// # Arguments
    ///
    /// * `names` - Names of the new schema.
    /// * `dtypes` - Dypes of the new schema.
    pub fn from_vecs(names: Vec<&str>, dtypes: Vec<DataType>) -> Self {
        let mut attrs = Vec::new();
        for (name, dtype) in names.iter().zip(dtypes.iter()) {
            attrs.push(Attribute::new(name.to_string(), dtype.clone()));
        }
        TableSchema::new(attrs)
    }

    /// Get the attribute from the given index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the attribute to look for.
    pub fn get_attribute(&self, i: usize) -> Option<&Attribute> {
        self.attributes.get(i)
    }

    /// Get the index of the attribute.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to get the index for.
    pub fn get_field_index(&self, name: &str) -> Option<&usize> {
        self.name_map.get(name)
    }

    /// Check if the attribute name is in the schema.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to look for.
    pub fn contains(&self, name: &str) -> bool {
        self.name_map.contains_key(name)
    }

    /// Get an iterator of the attributes.
    pub fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        self.attributes.iter()
    }

    /// Merge two schemas into one.
    ///
    /// The other schema is appended to the current schema.
    ///
    /// # Arguments
    ///
    /// * `other` - Other schema to add to current schema.
    pub fn merge(&self, other: &Self) -> Self {
        let mut attrs = self.attributes.clone();
        attrs.append(&mut other.attributes.clone());
        Self::new(attrs)
    }

    /// Returns the length of the schema.
    pub fn size(&self) -> usize {
        self.attributes.len()
    }

    /// Returns the size of the schema in bytes.
    pub fn byte_size(&self) -> usize {
        let mut total: usize = 0;
        for attr in self.attributes.iter() {
            total += attr.get_byte_len();
        }
        total
    }
}

/// Handle attributes. Pairs the name with the dtype.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Attribute {
    /// Attribute name.
    pub name: String,
    /// Attribute dtype.
    pub dtype: DataType,
}

impl Attribute {
    /// Create a new attribute with the given name and dtype.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute.
    /// * `dtype` - Dtype of the attribute.
    pub fn new(name: String, dtype: DataType) -> Self {
        Self { name, dtype }
    }

    /// Returns the name of the attribute.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the dtype of the attribute.
    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }

    // TODO(williamma12): Where does the 132 come from?
    /// Returns the length of the dtype in bytes.
    pub fn get_byte_len(&self) -> usize {
        match self.dtype {
            DataType::Int => 4,
            DataType::String => 132,
        }
    }
}

/// Enumerate the supported dtypes.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub enum DataType {
    Int,
    String,
}

/// For each of the dtypes, make sure that there is a corresponding field type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord, Clone, Hash)]
pub enum Field {
    IntField(i32),
    StringField(String),
}

impl Field {
    /// Function to convert a Tuple field into bytes for serialization
    ///
    /// This function always uses least endian byte ordering and stores strings in the format |string length|string contents|.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Field::IntField(x) => x.to_le_bytes().to_vec(),
            Field::StringField(s) => {
                let s_len: usize = s.len();
                let mut result = s_len.to_le_bytes().to_vec();
                let mut s_bytes = s.clone().into_bytes();
                let padding_len: usize = 128 - s_bytes.len();
                let pad = vec![0; padding_len];
                s_bytes.extend(&pad);
                result.extend(s_bytes);
                result
            }
        }
    }

    /// Unwraps integer fields.
    pub fn unwrap_int_field(&self) -> i32 {
        match self {
            Field::IntField(i) => *i,
            _ => panic!("Expected i32"),
        }
    }

    /// Unwraps string fields.
    pub fn unwrap_string_field(&self) -> &str {
        match self {
            Field::StringField(s) => &s,
            _ => panic!("Expected String"),
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Field::IntField(x) => write!(f, "{}", x),
            Field::StringField(x) => write!(f, "{}", x),
        }
    }
}

/// Record id type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct RecordId {
    /// File id that contains the record.
    pub file_id: u64,
    /// Page number that contains the record.
    pub page_no: u32,
    /// Record's index location in the page that contains the record.
    pub page_ind: usize,
}

impl RecordId {
    /// Create a new recordid with the given data.
    ///
    /// # Arguments
    ///
    /// * `file_id` - file id that contains the record.
    /// * `page_no` - page number that contains the record.
    /// * `page_ind` - record's index location in the page that contains the record.
    pub fn new(file_id: u64, page_no: u32, page_ind: usize) -> Self {
        Self {
            file_id,
            page_no,
            page_ind,
        }
    }
}

/// Tuple type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Tuple {
    /// Tuple data.
    pub field_vals: Vec<Field>,
    /// Tuple's location.
    pub record_id: RecordId,
}

impl Tuple {
    /// Create a new tuple with the given data.
    ///
    /// # Arguments
    ///
    /// * `field_vals` - Field values of the tuple.
    pub fn new(field_vals: Vec<Field>) -> Self {
        Self {
            field_vals,
            record_id: RecordId::new(0, 0, 0),
        }
    }

    /// Get the field at index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the field.
    pub fn get_field(&self, i: usize) -> Option<&Field> {
        self.field_vals.get(i)
    }

    /// Update the index at field.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the value to insert.
    /// * `f` - Value to add.
    ///
    /// # Panics
    ///
    /// Panics if the index is out-of-bounds.
    pub fn set_field(&mut self, i: usize, f: Field) {
        self.field_vals[i] = f;
    }

    /// Returns an iterator over the field values.
    pub fn field_vals(&self) -> impl Iterator<Item = &Field> {
        self.field_vals.iter()
    }

    /// Return the length of the tuple.
    pub fn size(&self) -> usize {
        self.field_vals.len()
    }

    /// Append another tuple with self.
    ///
    /// # Arguments
    ///
    /// * `other` - Other tuple to append.
    pub fn merge(&self, other: &Self) -> Self {
        let mut fields = self.field_vals.clone();
        fields.append(&mut other.field_vals.clone());
        Self::new(fields)
    }

    /// Return the record id of the tuple.
    pub fn get_record_id(&self) -> RecordId {
        self.record_id.clone()
    }

    pub fn get_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(&self).unwrap()
    }

    pub fn from_bytes(bytes: &Vec<u8>) -> Self {
        serde_cbor::from_slice(&bytes).unwrap()
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut res = String::new();
        for field in &self.field_vals {
            let val = match field {
                Field::IntField(i) => i.to_string(),
                Field::StringField(s) => s.to_string(),
            };
            res.push_str(&val);
            res.push('\t');
        }
        write!(f, "{}", res)
    }
}

/// Retrieve the name from the command parser object.
///
/// # Argument
///
/// * `name` - Name object from the command parser.
pub fn get_name(name: &ast::ObjectName) -> Result<String, CrustyError> {
    if name.0.len() > 1 {
        Err(CrustyError::CrustyError(String::from(
            "Error no . names supported",
        )))
    } else {
        Ok(name.0[0].clone())
    }
}

/// Retrieve the dtype from the command parser object.
///
/// # Argument
///
/// * `dtype` - Name object from the command parser.
pub fn get_attr(dtype: &ast::DataType) -> Result<DataType, CrustyError> {
    match dtype {
        ast::DataType::Int => Ok(DataType::Int),
        ast::DataType::Varchar(_) => Ok(DataType::String),
        //TODO append type
        _ => Err(CrustyError::CrustyError(String::from(
            "Unsupported data type ",
        ))),
    }
}

#[cfg(test)]
mod libtests {
    use super::*;
    use crate::testutil::*;

    #[test]
    fn test_tuple_bytes() {
        let tuple = int_vec_to_tuple(vec![0, 1, 0]);
        let tuple_bytes = tuple.get_bytes();
        let check_tuple: Tuple = Tuple::from_bytes(&tuple_bytes);
        assert_eq!(tuple, check_tuple);
    }
}
