use common::ids::Permissions;
use common::ids::*;
use common::storage_trait::StorageTrait;
use common::CrustyError;

use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// This is the basic data structure a container that maps a value ID to bytes
type ContainerMap = Arc<RwLock<HashMap<ValueId, Vec<u8>>>>;

/// The MemStore StorageManager. A map for storing containers, a map for tracking the next insert ID,
/// and where to persist on shutdown/startup
pub struct StorageManager {
    containers: Arc<RwLock<HashMap<ContainerId, ContainerMap>>>,
    last_insert: Arc<RwLock<HashMap<ContainerId, ValueId>>>,
    persist_path: PathBuf,
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        info!("Dropping Storage Manager");
    }
}
impl StorageTrait for StorageManager {
    type ValIterator = ValueIterator;

    /// Create a new SM from scratch or create containers from files.
    fn new(storage_path: String) -> Self {
        if storage_path != "" && Path::exists(Path::new(&storage_path)) {
            info!(
                "Initializing memstore::storage_manager from path: {:?}",
                &storage_path
            );
            StorageManager::load(storage_path)
        } else {
            info!(
                "Creating new memstore::storage_manager with path: {:?}",
                &storage_path
            );
            StorageManager {
                containers: Arc::new(RwLock::new(HashMap::new())),
                last_insert: Arc::new(RwLock::new(HashMap::new())),
                persist_path: PathBuf::from(storage_path),
            }
        }
    }

    /// Create a new SM that will not be persisted
    fn new_test_sm() -> Self {
        StorageManager::new(String::from(""))
    }

    /// Insert bytes into a container
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        _tid: TransactionId,
    ) -> ValueId {
        // Get the container
        let mut containers = self.containers.write().unwrap();
        // Find key to insert
        let mut last_insert = self.last_insert.write().unwrap();
        // Get the container map to allow the insert
        let mut vals = containers
            .get_mut(&container_id)
            .expect("Container ID Missing on insert")
            .write()
            .unwrap();
        let next_slot = match last_insert.get(&container_id) {
            None => 0,
            Some(slot) => slot.slot_id.expect("Missing SlotId") + 1,
        };
        //TODO check if exits first in case of mistake
        let rid = ValueId {
            container_id,
            segment_id: None,
            page_id: None,
            slot_id: Some(next_slot),
        };
        debug!("memstore:storage_manager insert key: {:?} value: {:?}", &rid, &value);
        vals.insert(rid, value);
        last_insert.insert(container_id, rid.clone());
        rid
    }

    /// Insert multiple values
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for x in values {
            ret.push(self.insert_value(container_id, x, tid));
        }
        ret
    }

    /// Remove the value from the container
    fn delete_value(&self, id: ValueId, _tid: TransactionId) -> Result<(), CrustyError> {
        let containers = self.containers.write().unwrap();
        if containers.contains_key(&id.container_id) {
            let mut table_map = containers.get(&id.container_id).unwrap().write().unwrap();
            if table_map.contains_key(&id) {
                table_map.remove(&id);
                Ok(())
            } else {
                //Key not found, no need to delete.
                return Ok(());
            }
        } else {
            return Err(CrustyError::CrustyError(String::from(
                "File ID not found for recordID",
            )));
        }
    }

    /// Updates a value. Returns record ID on update (which may have changed). Error on failure
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        self.delete_value(id, _tid)?;
        Ok(self.insert_value(id.container_id, value, _tid))
    }

    /// Add a new container
    fn create_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut containers = self.containers.write().unwrap();
        if containers.contains_key(&container_id) {
            debug!("memstore::create_container container_id: {:?} already exists", &container_id);
            return Ok(());
        }
        debug!("memstore::create_container container_id: {:?} does not exist yet", &container_id);
        containers.insert(container_id, Arc::new(RwLock::new(HashMap::new())));
        Ok(())
    }

    /// Remove the container and all stored values in the container. 
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut containers = self.containers.write().unwrap();
        if !containers.contains_key(&container_id) {
            debug!("memstore::remove_container container_id: {:?} does not exist", &container_id);
            return Ok(());
        }
        debug!("memstore::remove_container container_id: {:?} exists. dropping", &container_id);
        containers.remove(&container_id).unwrap();
        Ok(())
    }

    /// Get an iterator for a container
    fn get_iterator(
        &self,
        container_id: ContainerId,
        _tid: TransactionId,
        _perm: Permissions,
    ) -> ValueIterator {
        let table_map = self
            .containers
            .read()
            .unwrap()
            .get(&container_id)
            .unwrap()
            .clone();
        let last_insert = self.last_insert.read().unwrap();
        debug!("memstore::get_iterator container_id: {:?}", &container_id);
        let max = last_insert.get(&container_id).unwrap().slot_id.unwrap_or(0);
        ValueIterator::new(table_map, container_id, max)
    }

    /// Get the bytes for a given value if found
    fn get_value(
        &self,
        id: ValueId,
        _tid: TransactionId,
        _perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let containers = self.containers.read().unwrap();
        if containers.contains_key(&id.container_id) {
            let map = containers.get(&id.container_id).unwrap().read().unwrap();
            if map.contains_key(&id) {
                Ok(map.get(&id).unwrap().clone())
            } else {
                Err(CrustyError::ExecutionError(format!(
                    "Record ID not found {:?}",
                    id
                )))
            }
        } else {
            Err(CrustyError::ExecutionError(format!(
                "File ID not found {:?}",
                id
            )))
        }
    }

    fn transaction_finished(&self, _tid: TransactionId) {
        panic!("Not implemented");
    }

    fn reset(&self) {
        panic!("Not implemented");
    }

    fn shutdown(&self) {
        info!("Shutting down and persisting containers");
        if self.persist_path.to_string_lossy() == String::from("") {
            info!("Test SM or no path, not persisting");
            return;
        }
        fs::create_dir_all(self.persist_path.to_path_buf())
            .expect("Unable to create dir to store SM");
        let containers = self.containers.read().unwrap();
        for (c_id, vals_lock) in containers.iter() {
            let vals = vals_lock.read().unwrap();
            let mut file_path = self.persist_path.clone();
            file_path.push(format!("{}", c_id));
            file_path.set_extension("ms");
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(file_path)
                .expect("Failed to create file");
            serde_cbor::to_writer(file, &*vals).expect("Failed on persisting container");
        }
    }
}

impl StorageManager {
    /// Create a Memstore SM from a file path and populate from the files
    fn load(path: String) -> Self {
        let mut container_map = HashMap::new();
        let mut last_ins = HashMap::new();
        // Find the files that end with .ms
        let entries: Vec<fs::DirEntry> = fs::read_dir(&path)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|x| x.path().extension().unwrap() == "ms")
            .collect();
        // populate
        for entry in entries {
            // Open the file
            let file = OpenOptions::new()
                .read(true)
                .open(entry.path())
                .expect("Failed to read file");

            // Create the container be using serde to de-serialize the file
            let container: HashMap<ValueId, Vec<u8>> =
                serde_cbor::from_reader(file).expect("cannot read file");
            
            // The file name contains the CID
            let cid: ContainerId = entry
                .path()
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string()
                .parse::<ContainerId>()
                .unwrap();
            // Find the max key for the next insert key
            let mut max_val: ValueId = ValueId {
                container_id: cid,
                segment_id: None,
                page_id: None,
                slot_id: Some(0),
            };
            for key in container.keys() {
                if let Some(slot) = key.slot_id {
                    if slot > max_val.slot_id.unwrap() {
                        max_val = key.clone();
                    }
                }
            }
            container_map.insert(cid, Arc::new(RwLock::new(container)));
            last_ins.insert(cid, max_val);
        }
        StorageManager {
            containers: Arc::new(RwLock::new(container_map)),
            last_insert: Arc::new(RwLock::new(last_ins)),
            persist_path: PathBuf::from(path),
        }
    }
}

// The iterator struct
pub struct ValueIterator {
    tracker: ValueId,
    max: u16,
    table_map: ContainerMap,
    current: u16,
}

impl ValueIterator {
    //Create a new iterator for a container
    fn new(table_map: ContainerMap, container_id: ContainerId, max: u16) -> Self {
        debug!("new iterator {:?} max {}", container_id, max);
        let mut tracker = ValueId::new(container_id);
        tracker.slot_id = Some(0);
        ValueIterator {
            tracker,
            max,
            table_map,
            current: 0,
        }
    }
}

impl Iterator for ValueIterator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        while self.current <= self.max {
            match self.table_map.read().unwrap().get(&self.tracker) {
                Some(res) => {
                    self.tracker.slot_id = Some(self.tracker.slot_id.unwrap() + 1);
                    self.current += 1;
                    return Some(res.clone());
                }
                None => {
                    self.tracker.slot_id = Some(self.tracker.slot_id.unwrap() + 1);
                    self.current += 1;
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use common::ids::Permissions;
    use common::ids::TransactionId;
    use common::testutil::*;
    use common::Tuple;

    #[test]
    fn test_get_val1() {
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let sm = StorageManager::new_test_sm();
        let container_id = 1;
        sm.create_container(container_id).unwrap();
        let tid = TransactionId::new();
        let rid = sm.insert_value(container_id, tuple_bytes.clone(), tid);
        let check_bytes = sm.get_value(rid, tid, Permissions::ReadOnly).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
    }

    #[test]
    fn test_get_val2() {
        let tuple = int_vec_to_tuple(vec![0, 1, 0]);
        let tuple2 = int_vec_to_tuple(vec![0, 1, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        let sm = StorageManager::new_test_sm();
        let container_id = 1;
        sm.create_container(container_id).unwrap();
        let tid = TransactionId::new();
        let rid = sm.insert_value(container_id, tuple_bytes.clone(), tid);
        let rid2 = sm.insert_value(container_id, tuple_bytes2.clone(), tid);
        let mut check_bytes = sm.get_value(rid, tid, Permissions::ReadOnly).unwrap();
        let mut check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        check_bytes = sm.get_value(rid2, tid, Permissions::ReadOnly).unwrap();
        check_tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(tuple2, check_tuple);
    }

    #[test]
    fn test_multi() {
        let tuple = int_vec_to_tuple(vec![0, 1, 0]);
        let tuple2 = int_vec_to_tuple(vec![0, 1, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        let byte_vec = vec![tuple_bytes.clone(), tuple_bytes2.clone()];
        let sm = StorageManager::new_test_sm();
        let container_id = 1;
        sm.create_container(container_id).unwrap();
        let tid = TransactionId::new();
        let rid = sm.insert_values(container_id, byte_vec, tid);
        let mut check_bytes = sm
            .get_value(*rid.get(0).unwrap(), tid, Permissions::ReadOnly)
            .unwrap();
        let mut check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        check_bytes = sm
            .get_value(*rid.get(1).unwrap(), tid, Permissions::ReadOnly)
            .unwrap();
        check_tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(tuple2, check_tuple);
    }

    #[test]
    fn test_delete1() {
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let sm = StorageManager::new_test_sm();
        let container_id = 1;
        sm.create_container(container_id).unwrap();
        let tid = TransactionId::new();
        let rid = sm.insert_value(container_id, tuple_bytes.clone(), tid);
        let check_bytes = sm.get_value(rid, tid, Permissions::ReadOnly).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        let res = sm.delete_value(rid, tid);
        assert!(res.is_ok());
        let res2 = sm.get_value(rid, tid, Permissions::ReadOnly);
        assert!(res2.is_err());
    }

    #[test]
    fn test_simple_iter() {
        init();
        let tuple = int_vec_to_tuple(vec![0, 1, 0]);
        let tuple2 = int_vec_to_tuple(vec![0, 1, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        let sm = StorageManager::new_test_sm();
        let container_id = 1;
        sm.create_container(container_id).unwrap();
        let tid = TransactionId::new();
        let _rid = sm.insert_value(container_id, tuple_bytes.clone(), tid);
        let _rid2 = sm.insert_value(container_id, tuple_bytes2.clone(), tid);
        let mut iter = sm.get_iterator(container_id, tid, Permissions::ReadOnly);

        let mut check_bytes = iter.next().unwrap();
        let mut check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        check_bytes = iter.next().unwrap();
        check_tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(tuple2, check_tuple);
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_simple_iter_delete() {
        init();
        let tuple = int_vec_to_tuple(vec![0, 1, 0]);
        let tuple2 = int_vec_to_tuple(vec![0, 1, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        let sm = StorageManager::new_test_sm();
        let container_id = 1;
        sm.create_container(container_id).unwrap();
        let tid = TransactionId::new();
        let rid = sm.insert_value(container_id, tuple_bytes.clone(), tid);
        let _rid2 = sm.insert_value(container_id, tuple_bytes2.clone(), tid);
        let mut iter = sm.get_iterator(container_id, tid, Permissions::ReadOnly);

        let mut check_bytes = iter.next().unwrap();
        let mut check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        check_bytes = iter.next().unwrap();
        check_tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(tuple2, check_tuple);
        assert_eq!(None, iter.next());

        sm.delete_value(rid, tid).unwrap();
        let mut iter2 = sm.get_iterator(container_id, tid, Permissions::ReadOnly);
        check_bytes = iter2.next().unwrap();
        check_tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(tuple2, check_tuple);
        assert_eq!(None, iter2.next());
    }

    #[test]
    fn test_not_found() {
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let sm = StorageManager::new_test_sm();
        let container_id = 1;
        sm.create_container(container_id).unwrap();
        let tid = TransactionId::new();
        let rid = sm.insert_value(container_id, tuple_bytes.clone(), tid);
        let check_bytes = sm.get_value(rid, tid, Permissions::ReadOnly).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();

        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);
        let mut rid2 = ValueId::new(container_id);
        rid2.slot_id = Some(1000);
        assert!(
            sm.get_value(rid2, tid, Permissions::ReadOnly).is_err(),
            "value should not exist"
        );
        rid2.container_id = 1400;
        assert!(
            sm.get_value(rid2, tid, Permissions::ReadOnly).is_err(),
            "file should not exist"
        );
    }

    #[test]
    fn test_sm_shutdown() {
        init();
        let persist = gen_random_dir();
        info!("{:?}", persist);
        let sm = StorageManager::new(persist.to_string_lossy().to_string());
        let container_id = 1;
        sm.create_container(container_id).unwrap();

        sm.create_container(2).unwrap();
        let tid = TransactionId::new();
        let bytes1 = get_random_byte_vec(100);
        let bytes2 = get_random_byte_vec(300);
        let bytes3 = get_random_byte_vec(100);
        let vid1 = sm.insert_value(container_id, bytes1.clone(), tid);
        let vid2 = sm.insert_value(container_id, bytes2.clone(), tid);
        let vid3 = sm.insert_value(2, bytes3.clone(), tid);
        let vid4 = sm.insert_value(container_id, bytes2.clone(), tid);
        sm.delete_value(vid4, tid).unwrap();
        sm.shutdown();

        let sm2 = StorageManager::new(persist.to_string_lossy().to_string());
        let byte_check = sm2
            .get_value(vid1, tid, Permissions::ReadOnly)
            .expect("Can't get value");
        assert_eq!(bytes1[..], byte_check[..]);
        assert_eq!(
            bytes2[..],
            sm2.get_value(vid2, tid, Permissions::ReadOnly)
                .expect("Can't get value")[..]
        );
        assert_eq!(
            bytes3[..],
            sm2.get_value(vid3, tid, Permissions::ReadOnly)
                .expect("Can't get value")[..]
        );

        let vid5 = sm.insert_value(container_id, bytes2.clone(), tid);
        assert_eq!(vid4.slot_id.unwrap() + 1, vid5.slot_id.unwrap());

        fs::remove_dir_all(persist).unwrap();
    }
}
