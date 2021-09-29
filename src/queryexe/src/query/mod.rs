pub use executor::Executor;
pub use translate_and_validate::TranslateAndValidate;
mod executor;
mod translate_and_validate;

// Notes on Query Optimization
//    -- See the optimization-skeleton branch for an example
//       implementation of an optimization flow based on SimpleDb
//
// Query optimization would likely be done using a module whose
// functions are run in-between calls to TranslateAndValidate and
// Executor
// In server.rs calling the optimization functions might look something like this:
// server::DBServer::run_query() {
//       ...
//      Some(Some(db)) => {
//      let lp = TranslateAndValidate::from_sql(query, db)?;
//      let annotated_lp = Optimizer::new(lp, db);
//      Ok(Executor::new(db, &annotated_lp)?.execute())
//     ...
// }
//
// Putting optimization there would give optimization functions access to
// both the catalog and logical plan (currently a graph of logical relations).
//
// Actually writing the optimizer would likely involve processing a
// common-old::logical_plan::LogicalPlan. The function
// executor::Executor::logical_plan_to_op_iterator gives an example of
// recursively processing a logical plan graph.
//
// Alternatively, SimpleDb does query optimization by processing lists of logical
// operators.
//
// If the SimpleDB approach ends up being easier, the branch
// *** optimization-skeleton *** refactors the current flow so that
// TranslateAndValidate produces lists of logical operators, rather
// than a graph. Then, a separate function on the branch (inside
// optimizer.rs) turns the lists of operators into a graph, similar
// to the way SimipleDB's LogicalPlan.physicalPlan() function works.
//
// Note: the Executor expects a logical plan as input, so the
// optimizer would have to output a graph based annotated structure
// similar to a logical plan in order to process the plan using the
// current version of the executor. Otherwise, some of the executor
// may need to be rewritten to accommodate whatever new intermediate
// representation is chosen.  Hopefully, though, it will be easy to
// just create an annotated version of the LogicalPlan struct that the
// executor can process almost exactly like it currently processes
// LogicalPlans


