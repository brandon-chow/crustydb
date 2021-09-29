use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::cmp::{max, min};
use std::collections::HashMap;

/// Contains the index of the field to aggregate and the operator to apply to the column of each group.
#[derive(Clone)]
pub struct AggregateField {
    /* index: the index of the field to aggregate */
    index: usize,

    /* op: the operator to apply to the column of each group */
    op: AggOp
}


/// Computes an aggregation function over multiple columns and grouped by multiple fields.
struct Aggregator {
    agg_fields: Vec<AggregateField>,
    groupby_fields: Vec<usize>,
    groupby_hashmap: HashMap<Vec<Field>, Vec<Field>>,
    row_count: i32,
    schema: TableSchema
}

impl Aggregator {
    // TODO(williamma12): Add check that schema is set up correctly.
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(agg_fields: Vec<AggregateField>, groupby_fields: Vec<usize>, schema: TableSchema)
     -> Self {
            Self {
                agg_fields: agg_fields,
                groupby_fields: groupby_fields,
                groupby_hashmap: HashMap::new(),
                row_count: 0,
                schema: schema
            }
    }


    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {

        /* The key to search through the hashmap */
        let key = Vec::new();

        /* Loop through each field index in groupby_fields */
        for field_index in self.groupby_fields
        {
            /* Push the (field_index)th element of the tuple to the key */
            match tuple.get_field(field_index)
            {
                Some(&field) => key.push(field),
                None => break
            }
        }

        /* Handle the case where there is no group */
        /*if self.groupby_fields.len() == 0
        {
            /* Create an empty vector of 0 fields */
            let empty_field_vector = Vec::new();

            /* Push this empty vector into the key */
            key.push(empty_field_vector);
        }*/

        /* Get the value that corresponds with this key */
        let value = self.groupby_hashmap.get_mut(&key);

        /* Option is None if there are no values for this group yet */
        if value.is_none()
        {
            /* Create a vector that represents the
               value of the new key-value pair */
            let new_value = Vec::new();

            /* For each agg_field, push the (index)th value
               from the tuple into new_vector */
            for agg_field in self.agg_fields
            {
                match agg_field.op
                {
                    Count => new_value.push(Field::IntField(1)),
                    _ => new_value.push(tuple.field_vals[agg_field.index])
                }
            }

            /* Insert this new key-value pair into the groupby_hashmap HashMap */
            self.groupby_hashmap.insert(key, new_value);
        }

        /* Option is Some(x) so we can unwrap() it without running into errors*/
        else
        {
            /* Get the value from the hashmap that corresponds to the key */
            let current_value_vector = *value.unwrap();

            /* Loop through each AggregateField */
            for (index, agg_field) in self.agg_fields.iter().enumerate()
            {
                /* Find the (index)th value of the tuple */
                let tuple_element = tuple.field_vals[agg_field.index];

                let current_value = current_value_vector[index];

                /* Get the (index)th value from the current value for this key */
                match current_value
                {
                    /* Handle the case where current_value is an integer */
                    Field::IntField(current_value_int) =>

                        match tuple_element
                        {
                            Field::IntField(tuple_element_int) =>

                                /* Update current_value given the agg_field operator */
                                match agg_field.op
                                {
                                    AggOp::Avg =>
                                        current_value_int = (current_value_int * self.row_count) + tuple_element_int / (self.row_count + 1),
                                    AggOp::Count => current_value_int += 1,
                                    AggOp::Max =>
                                        if tuple_element_int > current_value_int
                                        {
                                            current_value_int = tuple_element_int;
                                        },
                                    AggOp::Min =>
                                        if tuple_element_int < current_value_int
                                        {
                                            current_value_int = tuple_element_int;
                                        },
                                    AggOp::Sum => current_value_int += tuple_element_int,
                                    _ => panic!("merge_tuple_into_group: Invalid operator")
                                },
                            Field::StringField(str) =>
                                match agg_field.op
                                {
                                    AggOp::Count => current_value_int += 1,
                                    _ => panic!("merge_tuple_into_group: Invalid operator")
                                },
                        },
                        _ => panic!("merge_tuple_into_group: current_value should be a number")
                }
            }
        }

        /* Increment row_count since we've processed one more row */
        self.row_count +=1;
    }


    // TODO: Create check for schema.
    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {

        /* Make an empty vector of tuples to be passed into the TupleIterator */
        let tuple_vector = Vec::new();

        /* Turn each key-value pair into a tuple and append it to the tuple_vector vector */
        for (key, val) in self.groupby_hashmap.iter()
        {
            let field_vector = Vec::new();
            for key_element in key
            {
                field_vector.push(*key_element);
            }
            for val_element in val
            {
                field_vector.push(*val_element);
            }
            tuple_vector.push(Tuple::new(field_vector));
        }

        /* Return a TupleIterator containing the vector of tuples */
        return TupleIterator::new(tuple_vector, self.schema);
    }
}

/// Aggregate operator.
pub struct Aggregate {

    groupby_indices: Vec<usize>,
    groupby_names: Vec<String>,
    agg_indices: Vec<usize>,
    agg_names: Vec<String>,
    ops: Vec<AggOp>,

    tuples: Vec<Tuple>,
    index: Option<usize>,

    /// Resulting schema.
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `schema` - Input schema.
    pub fn new(groupby_indices: Vec<usize>, groupby_names: Vec<&str>, agg_indices: Vec<usize>, agg_names: Vec<&str>, ops: Vec<AggOp>, schema: TableSchema) -> Self {

        Self {
            groupby_indices,
            groupby_names,
            agg_indices,
            agg_names,
            ops,
            tuples: Vec::new(),
            index: None,
            schema,
            open: false
        }
    }

}


impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {

        /* Create an empty vector of AggregateField structs */
        let agg_fields = Vec::new();


        /* Enumerate through each index of agg_indices and push a new AggregateField struct */
        for (num, index) in self.agg_indices.iter().enumerate()
        {
            /* Initialize a new AggregateField given the index and the operator */
            let agg_field = AggregateField{
                index: *index,
                op: self.ops[num]
            };
            agg_fields.push(agg_field)
        }

        /* Initialize a new aggregator struct for this aggregation */
        let aggregator = Aggregator::new(agg_fields, self.groupby_indices, self.schema);

        /* Create the iterator for this aggregator */
        let iterator = aggregator.iterator();

        /* Push all the tuples into the tuples field of the aggregate struct */

        match iterator.open()
        {
            Ok(()) => (),
            Err(error) => return Err(error)
        }

        loop
        {
            match iterator.next()
            {
                Ok(Some(tuple)) => self.tuples.push(tuple),
                Ok(None) => break,
                Err(error) => return Err(error)
            }
        }

        match iterator.close()
        {
            Ok(()) => (),
            Err(error) => return Err(error)
        }
        self.open = true;
        return Ok(());
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {

        let i = match self.index
        {
            None => panic!("Operator not open"),
            Some(i) => i,
        };
        let tuple = self.tuples.get(i);
        self.index = Some(i + 1);
        return Ok(tuple.cloned());
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.index = None;
        self.open = false;
        return Ok(());
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if self.index.is_none() {
            panic!("Operator has not been opened")
        }
        self.close();
        return self.open();
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField{index: field, op: op}], Vec::new(), schema); /* EDITED */

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() -> () {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(schema
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while let Some(_) = iter.next()? {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            col: usize,
            expected: i32,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
            );
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, 0, 6)?;
            test_single_agg_no_group(AggOp::Sum, 0, 21)?;
            test_single_agg_no_group(AggOp::Max, 0, 6)?;
            test_single_agg_no_group(AggOp::Min, 0, 1)?;
            test_single_agg_no_group(AggOp::Avg, 0, 3)?;
            test_single_agg_no_group(AggOp::Count, 3, 6)
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
            );
            ai.open()?;
            let first_row: Vec<Field> = ai
                .next()?
                .unwrap()
                .field_vals()
                .map(|f| f.clone())
                .collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().map(|f| f.clone()).collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                Field::IntField(1),
                Field::IntField(3),
                Field::IntField(2),
                Field::IntField(2),
            ];
            assert_eq!(expected, result[0]);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "avg", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
