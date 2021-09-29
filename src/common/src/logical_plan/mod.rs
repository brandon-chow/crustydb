use crate::crusty_graph::{CrustyGraph, NodeIndex};
use crate::CrustyError;
pub use logical_op::*;
use serde_json::{json, Value};
use std::collections::HashMap;
mod logical_op;
use std::default::Default;
use std::fmt;

/// OpIndex is used to identify nodes in the LogicalPlan.
pub type OpIndex = NodeIndex;

/// Graph where nodes represent logical operations and edges represent the flow of data.
pub struct LogicalPlan {
    /// Graph of the logical plan.
    dataflow: CrustyGraph<LogicalOp>,
    /// The root represents final output operation. Root does not work if the graph contains any unconnected components.
    root: Option<OpIndex>,
}

impl Default for LogicalPlan {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalPlan {
    /// Creates an empty logical plan.
    pub fn new() -> Self {
        Self {
            dataflow: CrustyGraph::new(),
            root: None,
        }
    }

    /// Adds a node with an associated LogicalOp to the logical plan and returns the index of the added node.
    ///
    /// # Arguments
    ///
    /// * `operator` - Operator to add to the logical plan.
    pub fn add_node(&mut self, operator: LogicalOp) -> OpIndex {
        let index = self.dataflow.add_node(operator);
        if self.root.is_none() {
            self.root = Some(index)
        }
        index
    }

    /// Adds from source to target.
    ///
    /// In the logical plan representation data flows from target to source.
    ///
    /// # Arguments
    ///
    /// * `source` - Data producer.
    /// * `target` - Data consumer.
    pub fn add_edge(&mut self, source: OpIndex, target: OpIndex) {
        if let Some(index) = self.root {
            if index == target {
                self.root = Some(source);
            }
        }
        self.dataflow.add_edge(source, target);
    }

    /// Returns an iterator over all nodes that 'from' has an edge to.
    ///
    /// # Arguments
    ///
    /// * `from` - Node to get the edges of.
    pub fn edges<'a>(&'a self, from: OpIndex) -> impl Iterator<Item = NodeIndex> + 'a {
        self.dataflow.edges(from)
    }

    /// Gets the index of the root node, if such a node is present.
    ///
    /// The root node represents the final output operation in the logical plan.
    pub fn root(&self) -> Option<OpIndex> {
        self.root
    }

    /// Returns the LogicalOperation associated with a node.
    ///
    /// # Arguments
    ///
    /// * `index` - Index of the node to get the logical operation of.
    pub fn get_operator(&self, index: OpIndex) -> Option<&LogicalOp> {
        self.dataflow.node_data(index)
    }

    /// Returns the total number of nodes present in the graph.
    pub fn node_count(&self) -> usize {
        self.dataflow.node_count()
    }

    /// Returns the total number of edges present in the graph.
    pub fn edge_count(&self) -> usize {
        self.dataflow.edge_count()
    }

    /// Serializes the Logical Plan as json.
    pub fn to_json(&self) -> serde_json::Value {
        let mut node_map = HashMap::new();
        let mut edge_map = HashMap::new();
        for (i, node) in self.dataflow.node_references() {
            node_map.insert(i, node.data());
        }
        for (_, edge) in self.dataflow.edge_references().enumerate() {
            let source = edge.source();
            let targets = edge_map.entry(source).or_insert_with(Vec::new);
            targets.push(edge.target().to_string());
        }
        return json!({"nodes":node_map,
                      "edges":edge_map,
                      "root":self.root.map(|i| i.to_string())});
    }

    fn map_crusty_err<T>(
        result: serde_json::Result<T>,
        err: CrustyError,
    ) -> Result<T, CrustyError> {
        match result {
            Ok(res) => Ok(res),
            _ => Err(err),
        }
    }

    /// De-Serializes a json representation of the Logical Plan created in to_json
    pub fn from_json(json: &str) -> Result<Self, CrustyError> {
        let malformed_err =
            CrustyError::CrustyError(String::from("Malformatted logical plan json"));
        let v: Value =
            LogicalPlan::map_crusty_err(serde_json::from_str(json), malformed_err.clone())?;
        let nodes: HashMap<String, LogicalOp> = LogicalPlan::map_crusty_err(
            serde_json::from_value(v["nodes"].clone()),
            malformed_err.clone(),
        )?;
        let edges: HashMap<String, Vec<String>> = LogicalPlan::map_crusty_err(
            serde_json::from_value(v["edges"].clone()),
            malformed_err.clone(),
        )?;
        let root: Option<String> = LogicalPlan::map_crusty_err(
            serde_json::from_value(v["root"].clone()),
            malformed_err.clone(),
        )?;
        let mut graph_map = HashMap::new();
        let mut plan = LogicalPlan::new();
        for (i, val) in nodes.iter() {
            let node = plan.dataflow.add_node(val.clone());
            graph_map.insert(i, node);
        }

        if let Some(i) = root {
            let root_node = graph_map.get(&i).ok_or_else(|| malformed_err.clone())?;
            plan.root = Some(*root_node);
        }

        for (source, targets) in edges.iter() {
            let source_node = graph_map.get(source).ok_or_else(|| malformed_err.clone())?;
            for target in targets {
                let target_node = graph_map
                    .get(&target.to_string())
                    .ok_or_else(|| malformed_err.clone())?;
                plan.dataflow.add_edge(*source_node, *target_node);
            }
        }
        Ok(plan)
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_json())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_new() {
        let lp = LogicalPlan::new();
        assert_eq!(lp.node_count(), 0);
        assert_eq!(lp.edge_count(), 0);
        assert_eq!(lp.root, None);
    }

    #[test]
    fn test_add_node() {
        let count = 10;
        let mut lp = LogicalPlan::new();
        for i in 0..count {
            lp.add_node(LogicalOp::Scan(ScanNode {
                alias: i.to_string(),
            }));
        }
        assert_eq!(lp.node_count(), count);
    }

    #[test]
    fn test_add_edge() {
        let count = 10;
        let mut lp = LogicalPlan::new();
        let mut prev = lp.add_node(LogicalOp::Scan(ScanNode {
            alias: 0.to_string(),
        }));
        for i in 0..count {
            let curr = lp.add_node(LogicalOp::Scan(ScanNode {
                alias: i.to_string(),
            }));
            lp.add_edge(curr, prev);
            prev = curr;
        }
        assert_eq!(lp.root, Some(prev));
        assert_eq!(lp.edge_count(), count);
    }

    #[test]
    fn test_add_two_edges() {
        let mut lp = LogicalPlan::new();
        let parent = lp.add_node(LogicalOp::Scan(ScanNode {
            alias: String::from("parent"),
        }));
        let child1 = lp.add_node(LogicalOp::Scan(ScanNode {
            alias: String::from("child1"),
        }));
        let child2 = lp.add_node(LogicalOp::Scan(ScanNode {
            alias: String::from("child2"),
        }));
        lp.add_edge(parent, child1);
        lp.add_edge(parent, child2);
        assert_eq!(lp.edge_count(), 2);
    }

    #[test]
    fn test_edges() {
        let mut lp = LogicalPlan::new();
        let parent = lp.add_node(LogicalOp::Scan(ScanNode {
            alias: String::from("parent"),
        }));
        let child1 = lp.add_node(LogicalOp::Scan(ScanNode {
            alias: String::from("child1"),
        }));
        let child2 = lp.add_node(LogicalOp::Scan(ScanNode {
            alias: String::from("child2"),
        }));
        lp.add_edge(parent, child1);
        lp.add_edge(parent, child2);
        let mut edges = lp.edges(parent);
        assert_eq!(edges.next(), Some(child2));
        assert_eq!(edges.next(), Some(child1));
    }

    #[test]
    fn test_get_operator() {
        let count = 5;
        let mut nodes = Vec::new();
        let mut lp = LogicalPlan::new();
        for i in 0..count {
            let index = lp.add_node(LogicalOp::Scan(ScanNode {
                alias: i.to_string(),
            }));
            nodes.push(index);
        }

        for i in 0..count {
            let expected = i.to_string();
            match lp.get_operator(nodes[i]) {
                Some(LogicalOp::Scan(s)) => {
                    assert_eq!(expected, s.alias);
                }
                _ => panic!("Incorrect operator"),
            }
        }
    }

    #[test]
    fn test_json() {
        let mut lp = LogicalPlan::new();
        let scan = lp.add_node(LogicalOp::Scan(ScanNode {
            alias: String::from("Table"),
        }));
        let project = lp.add_node(LogicalOp::Project(ProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        }));
        lp.add_edge(project, scan);
        let json = lp.to_json();
        let new_lp = LogicalPlan::from_json(&json.to_string()).unwrap();
        assert_eq!(lp.dataflow.node_count(), new_lp.dataflow.node_count());
        assert_eq!(lp.dataflow.edge_count(), new_lp.dataflow.edge_count());

        let original_root = lp.dataflow.node_data(lp.root.unwrap()).unwrap();
        let new_root = lp.dataflow.node_data(lp.root.unwrap()).unwrap();
        match (original_root, new_root) {
            (LogicalOp::Project(_), LogicalOp::Project(_)) => (),
            _ => panic!("Incorrect root"),
        }
    }
}
