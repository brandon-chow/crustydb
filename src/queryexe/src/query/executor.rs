use crate::opiterator::*;
use crate::StorageManager;
use common::catalog::Catalog;
use common::ids::TransactionId;
use common::logical_plan::*;
use common::table::*;
use common::{CrustyError, QueryResult, TableSchema, Tuple};
use std::sync::Arc;

/// Manages the execution of queries using OpIterators and converts a LogicalPlan to a tree of OpIterators and runs it.
pub struct Executor {
    /// Executor state
    pub plan: Option<Box<dyn OpIterator>>,
    pub storage_manager: Option<Arc<StorageManager>>,
}

impl Executor {
    /// Initializes an executor.
    ///
    /// Takes in the database catalog, query's logical plan, and transaction id to create the
    /// physical plan for the executor.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Catalog of the database containing the metadata about the tables and such.
    /// * `storage_manager` - The SM for the DB to get access to files/buffer pool
    /// * `logical_plan` - Translated logical plan of the query.
    /// * `tid` - Id of the transaction that this executor is running.
    pub fn new_ref() -> Self {
        Self {
            plan: None,
            storage_manager: None,
        }
    }

    pub fn configure_sm(&mut self, storage_manager: &Arc<StorageManager>) {
        self.storage_manager = Some(storage_manager.clone());
    }

    pub fn configure_query(&mut self, physical_plan: Box<dyn OpIterator>) {
        self.plan = Some(physical_plan);
    }

    /// Returns the physical plan iterator to begin execution.
    pub fn start(&mut self) -> Result<(), CrustyError> {
        self.plan.as_mut().unwrap().open()
    }

    /// Returns the next tuple or None if there is no such tuple.
    ///
    /// # Panics
    ///
    /// Panics if physical plan iterator is closed
    pub fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        self.plan.as_mut().unwrap().next()
    }

    /// Closes the physical plan iterator.
    pub fn close(&mut self) -> Result<(), CrustyError> {
        self.plan.as_mut().unwrap().close()
    }

    /// Consumes the physical plan iterator and stores the result in a QueryResult.
    pub fn execute(&mut self) -> Result<QueryResult, CrustyError> {
        let schema = self.plan.as_mut().unwrap().get_schema();
        // TODO: Deal with the magic numbers.
        let width = schema
            .attributes()
            .map(|a| a.name().len())
            .max()
            .unwrap_or(10)
            + 2;
        let mut res = String::new();
        for attr in schema.attributes() {
            let s = format!("{:width$}", attr.name(), width = width);
            res += &s;
        }
        res += "\n";

        &self.start()?;
        while let Some(t) = &self.next()? {
            for f in t.field_vals() {
                let s = format!("{:width$}", f.to_string(), width = width);
                res += &s;
            }
            res += "\n";
        }
        &self.close()?;
        Ok(QueryResult::new(&res))
    }

    /// Converts a logical_plan to a physical_plan of op_iterators.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Catalog of the database containing the metadata about the tables and such.
    /// * `logical_plan` - Translated logical plan of the query.
    /// * `tid` - Id of the transaction that this executor is running.
    pub fn logical_plan_to_op_iterator<T: Catalog>(
        storage_manager: &Arc<StorageManager>,
        catalog: &T,
        lp: &LogicalPlan,
        tid: TransactionId,
    ) -> Result<Box<dyn OpIterator>, CrustyError> {
        let start = lp
            .root()
            .ok_or_else(|| CrustyError::ExecutionError(String::from("No root node")))?;
        Executor::logical_plan_to_op_iterator_helper(&storage_manager, catalog, lp, start, tid)
    }

    /// Recursive helper function to parse logical plan into physical plan.
    ///
    /// Function first converts all of the current nodes children to a physical plan before converting self to a physical plan.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Catalog of the database containing the metadata about the tables and such.
    /// * `logical_plan` - Translated logical plan of the query.
    /// * `tid` - Id of the transaction that this executor is running.
    fn logical_plan_to_op_iterator_helper<T: Catalog>(
        storage_manager: &Arc<StorageManager>,
        catalog: &T,
        lp: &LogicalPlan,
        start: OpIndex,
        tid: TransactionId,
    ) -> Result<Box<dyn OpIterator>, CrustyError> {
        let err = CrustyError::ExecutionError(String::from("Malformed logical plan"));

        // Recursively convert the children in node of logical plan to physical plan.
        let mut children = lp.edges(start).map(|n| {
            Executor::logical_plan_to_op_iterator_helper(&storage_manager, catalog, lp, n, tid)
        });

        // Converts the current node in logical plan to a node in the physical plan.
        let op = lp.get_operator(start).ok_or_else(|| err.clone())?;
        let result: Result<Box<dyn OpIterator>, CrustyError> = match op {
            LogicalOp::Scan(ScanNode { alias }) => {
                let alias_id = Table::get_table_id(alias);
                let table = catalog.get_table_ptr(alias_id)?;
                Ok(Box::new(SeqScan::new(
                    storage_manager.clone(),
                    table,
                    &alias,
                    tid,
                )))
            }
            LogicalOp::Project(ProjectNode { identifiers }) => {
                let child = children.next().ok_or_else(|| err.clone())??;
                match &identifiers {
                    ProjectIdentifiers::Wildcard => {
                        let field_indices = (0..child.get_schema().size()).collect::<Vec<usize>>();
                        let project_iterator = ProjectIterator::new(field_indices, child);
                        // Ok(Box::new(ProjectIterator::new(field_indices, child)))
                        Ok(Box::new(project_iterator))
                    }
                    ProjectIdentifiers::List(identifiers) => {
                        let (indices, names) =
                            Self::get_field_indices_names(identifiers, child.get_schema())?;
                        let project_iterator =
                            ProjectIterator::new_with_aliases(indices, names, child);
                        Ok(Box::new(project_iterator))
                    }
                }
            }
            LogicalOp::Aggregate(AggregateNode { fields, group_by }) => {
                let child = children.next().ok_or_else(|| err.clone())??;
                let mut agg_fields = Vec::new();
                let mut ops = Vec::new();
                for field in fields {
                    if let Some(op) = field.agg_op() {
                        ops.push(op);
                        agg_fields.push(field.clone());
                    }
                }
                let (agg_indices, agg_names) =
                    Self::get_field_indices_names(&agg_fields, child.get_schema())?;
                let (groupby_indices, groupby_names) =
                    Self::get_field_indices_names(group_by, child.get_schema())?;
                let attributes = groupby_names.clone();
                attributes.append(&mut agg_names.clone());
                let agg = Aggregate::new(groupby_indices, groupby_names, agg_indices, agg_names, ops, TableSchema::new(attributes));
                Ok(Box::new(agg))
            }
            LogicalOp::Join(JoinNode {
                left, op, right, ..
            }) => {
                let left_child = children.next().ok_or_else(|| err.clone())??;
                let left_schema = left_child.get_schema();
                let right_child = children.next().ok_or_else(|| err.clone())??;
                let right_schema = right_child.get_schema();

                // Sometimes the join condition is written in reverse of the join tables order.
                if !left_schema.contains(left.column()) {
                    let left_index = Executor::get_field_index(left.column(), right_schema)?;
                    let right_index = Executor::get_field_index(right.column(), left_schema)?;
                    Ok(Box::new(Join::new(
                    )))
                } else {
                    let left_index = Executor::get_field_index(left.column(), left_schema)?;
                    let right_index = Executor::get_field_index(right.column(), right_schema)?;
                    Ok(Box::new(Join::new(
                    )))
                }
            }
            LogicalOp::Filter(FilterNode { predicate, .. }) => {
                let child = children.next().ok_or_else(|| err.clone())??;
                let (identifier, op, operand) = match (&predicate.left, &predicate.right) {
                    (PredExpr::Ident(i), PredExpr::Literal(f)) => (i, predicate.op, f),
                    (PredExpr::Literal(f), PredExpr::Ident(i)) => (i, predicate.op.flip(), f),
                    _ => {
                        return Err(err.clone());
                    }
                };
                let idx = Executor::get_field_index(identifier.column(), child.get_schema())?;
                let filter = Filter::new(op, idx, operand.clone(), child);
                Ok(Box::new(filter))
            }
        };

        if children.next().is_some() {
            Err(err)
        } else {
            result
        }
    }

    /// Get the index of the column in the schema.
    ///
    /// # Arguments
    ///
    /// * `col` - Column name to find the index of.
    /// * `schema` - Schema to look for the column in.
    fn get_field_index(col: &str, schema: &TableSchema) -> Result<usize, CrustyError> {
        schema
            .get_field_index(col)
            .copied()
            .ok_or_else(|| CrustyError::ExecutionError(String::from("Unrecognized column name")))
    }

    // TODO: Fix test cases to be able to address the clippy warning of pointer arguments.
    /// Finds the column indices and names of column alias present in the given schema.
    ///
    /// # Arguments
    ///
    /// * `fields` - Vector of column names to look for.
    /// * `schema` - Schema to look for the column names in.
    #[allow(clippy::ptr_arg)]
    fn get_field_indices_names<'b>(
        fields: &'b Vec<FieldIdentifier>,
        schema: &TableSchema,
    ) -> Result<(Vec<usize>, Vec<&'b str>), CrustyError> {
        let mut field_indices = Vec::new();
        let mut field_names = Vec::new();
        for f in fields.iter() {
            let i = Executor::get_field_index(f.column(), schema)?;
            field_indices.push(i);
            let new_name = f.alias().unwrap_or_else(|| f.column());
            field_names.push(new_name)
        }
        Ok((field_indices, field_names))
    }
}

/* FIXME
#[cfg(test)]
mod test {
    use super::super::test::*;
    use super::*;
    use crate::bufferpool::*;
    use crate::DBSERVER;
    use common::{DataType, Field, TableSchema};

    fn test_logical_plan() -> LogicalPlan {
        let mut lp = LogicalPlan::new();
        let scan = LogicalOp::Scan(ScanNode {
            alias: TABLE_A.to_string(),
        });
        let project = LogicalOp::Project(ProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        });
        let si = lp.add_node(scan);
        let pi = lp.add_node(project);
        lp.add_edge(pi, si);
        lp
    }

    #[test]
    fn test_to_op_iterator() -> Result<(), CrustyError> {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut op = Executor::logical_plan_to_op_iterator(&db, &lp, tid).unwrap();
        op.open()?;
        let mut sum = 0;
        while let Some(t) = op.next()? {
            for i in 0..t.size() {
                sum += match t.get_field(i).unwrap() {
                    Field::IntField(n) => n,
                    _ => panic!("Not an IntField"),
                }
            }
        }
        assert_eq!(sum, TABLE_A_CHECKSUM);
        DBSERVER.transaction_complete(tid, true).unwrap();
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_next_not_started() {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut executor = Executor::new(&db, &lp, tid).unwrap();
        executor.next().unwrap();
    }

    #[test]
    fn test_next() -> Result<(), CrustyError> {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut executor = Executor::new(&db, &lp, tid).unwrap();
        executor.start()?;
        let mut sum = 0;
        while let Some(t) = executor.next()? {
            for i in 0..t.size() {
                sum += *match t.get_field(i).unwrap() {
                    Field::IntField(n) => n,
                    _ => panic!("Not an IntField"),
                }
            }
        }
        println!("sum: {}", sum);
        assert_eq!(sum, TABLE_A_CHECKSUM);
        DBSERVER.transaction_complete(tid, true).unwrap();
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_close() {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut executor = Executor::new(&db, &lp, tid).unwrap();
        executor.start().unwrap();
        executor.close().unwrap();
        executor.next().unwrap();
    }

    #[test]
    fn test_get_field_indices_names() -> Result<(), CrustyError> {
        let names = vec!["one", "two", "three", "four"];
        let aliases = vec!["1", "2", "3", "4"];
        let indices = vec![0, 1, 2, 3];
        let types = std::iter::repeat(DataType::Int).take(4).collect();
        let schema = TableSchema::from_vecs(names.clone(), types);

        // Test without aliases.
        let fields = names.iter().map(|s| FieldIdent::new("", s)).collect();
        let (actual_indices, actual_names) = Executor::get_field_indices_names(&fields, &schema)?;
        assert_eq!(actual_indices, indices);
        assert_eq!(actual_names, names);

        // Test with aliases.
        let fields = names
            .iter()
            .zip(aliases.iter())
            .map(|(n, a)| FieldIdent::new_column_alias("", n, a))
            .collect();
        let (actual_indices, actual_names) = Executor::get_field_indices_names(&fields, &schema)?;
        assert_eq!(actual_indices, indices);
        assert_eq!(actual_names, aliases);
        Ok(())
    }
}
*/
