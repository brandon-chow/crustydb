use crate::ids::Permissions;
use crate::ids::*;
use crate::CrustyError;

// TODO: What does ContainerId add as a type? If nothing, then make it u16 and make it easier for clients of
// TODO: storage managers to use them

/// The trait for a storage manager in crustyDB.
/// A StorageManager should impl Drop also so a storage manager can clean up on shut down and
/// for testing storage managers to remove any state.
pub trait StorageTrait {
    /// The associated type of the iterator that will need to be written and defined for the storage manager
    /// This iterator will be used to scan records of a container
    type ValIterator: Iterator<Item = Vec<u8>>;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self;

    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self;

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId;

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId>;

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError>;

    /// Updates a value. Returns record ID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        tid: TransactionId,
    ) -> Result<ValueId, CrustyError>;

    /// Create a new container to be stored. The name must be unique. Multiple calls for the same name
    /// should return the same containerId.
    // fn create_container(&self, name: String) -> ContainerId;
    fn create_container(&self, container_id: ContainerId) -> Result<(), CrustyError>;

    /// Remove the container and all stored values in the container. 
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError>;

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Self::ValIterator;

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError>;

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId);

    /// Reset all state associated the storage manager.
    fn reset(&self);

    /// Call shutdown to persist state or clean up. Will be called by drop in addition to explicitly.
    fn shutdown(&self);
}
