use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::ids::{ContainerId, PageId, Permissions, TransactionId, ValueId};
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_dir;
use common::{CrustyError, PAGE_SIZE};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

/// The StorageManager struct
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_path: String,
    is_temp: bool,

    /* Lock to a vector of heapfile structs */
    heapfiles_lock: Arc<RwLock<Vec<Arc<HeapFile>>>>,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        /* Get a pointer to the heapfiles vector */
        let heapfiles = self.heapfiles_lock.clone().read().unwrap().clone();

        /* Try to read the page from the heapfile given the provided page_id */
        match self.lookup_hf(heapfiles, container_id).clone().read_page_from_file(page_id)
        {
            /* read_page_from_file succeeded: return the page wrapped in an option */
            Ok(page) => return Some(page),

            /* read_page_from_file failed: return None */
            Err(error) => return None
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        /* Get a pointer to the heapfiles vector */
        let heapfiles = self.heapfiles_lock.clone().write().unwrap().clone();

        /* Try to write the page to the heapfile */
        match self.lookup_hf(heapfiles, container_id).clone().write_page_to_file(page)
        {
            /* write_page_to_file succeeded: return Ok(()) */
            Ok(()) => return Ok(()),

            /* write_page_to_file failed: return a CrustyError */
            Err(err) => return Err(CrustyError::CrustyError(String::from(
                "write_page: could not write page to file")))
        }
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        /* Get a pointer to the heapfiles vector */
        let heapfiles = self.heapfiles_lock.clone().read().unwrap().clone();

        /* Return the number of pages for the heapfile with the proivded container_id */
        return self.lookup_hf(heapfiles, container_id).clone().num_pages();
    }

    // Iterate through all heapfiles in the SM to find the heapfile with the container_id
    // Takes in a valid container_id
    // Returns an index to the heapfile
    fn lookup_hf(&self, heapfiles: Vec<Arc<HeapFile>>, container_id: ContainerId) -> Arc<HeapFile> {
        /* Loop through the heapfiles vector to find the heapfile with the given container_id */
        for i in 0..heapfiles.len()
        {
            /* Clone the heapfiles[i] pointer */
            let heapfile = heapfiles[i].clone();

            /* Return the heapfile if has the required ContainerId */
            if HeapFile::container_id(&heapfile) == container_id
            {
                /* Return a clone of the heapfile */
                return heapfile.clone();
            }
        }
        panic!("lookup_hf: Invalid container_id");
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        /* Get a pointer to the heapfiles vector */
        let heapfiles = self.heapfiles_lock.clone().read().unwrap().clone();

        /* Get a pointer to the heapfile whose container_id matches the argument container_id */
        let heapfile = self.lookup_hf(heapfiles, container_id).clone();
        return (0,0); /* TODO */
        /*return(
                heapfiles[index].clone().read_count.load(Ordering::Relaxed),
                heapfiles[index].clone().write_count.load(Ordering::Relaxed)
        );*/
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self {
        let heapfiles_lock = Arc::new(RwLock::new(Vec::new()));
        let sm = StorageManager {
            storage_path: storage_path,
            is_temp: false,
            heapfiles_lock: heapfiles_lock
        };
        return sm;
    }

    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        let heapfiles_lock = Arc::new(RwLock::new(Vec::new()));
        let sm = StorageManager {
            storage_path: storage_path,
            is_temp: true,
            heapfiles_lock: heapfiles_lock
        };
        return sm;
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        /* Panic if value contains more than PAGE_SIZE bytes */
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        /* Create a new value_id struct whose container_id is set to the given container_id */
        let mut value_id = ValueId {
            container_id: container_id,
            segment_id: None,
            page_id: None,
            slot_id: None
        };

        /* Get a pointer to the heapfiles vector */
        let heapfiles = self.heapfiles_lock.clone().write().unwrap().clone();

        /* Get a pointer to the heapfile whose container_id matches the argument container_id */
        let heapfile = self.lookup_hf(heapfiles, container_id).clone();

        /* Get the number of pages in the heapfile */
        let num_pages = heapfile.num_pages();

        /* Try to insert the value in a page that already exists */
        for page_id in 0..num_pages
        {
            /* Try to read the page with page_id from the heapfile */
            match heapfile.read_page_from_file(page_id)
            {
                /* Read_page_from_file succeeded */
                Ok(mut page) =>

                    /* Check if we can add value into the page */
                    match page.add_value(&value)
                    {
                        /* add_value succeeded: update value_id and write edited page to file */
                        Some(slot_id) => {value_id.page_id = Some(page_id);
                                          value_id.slot_id = Some(slot_id);
                                          match heapfile.write_page_to_file(page)
                                          {
                                              Ok(()) => (),
                                              Err(error) => panic!("insert_value: could not write page to file")
                                          };

                                          /* break once we have have space for the value */
                                          break},

                        /* add_value failed: try adding the value to the next available page */
                        None => ()
                    },

                /* Read_page_from_file failed */
                Err(error) => panic!("insert_value: Invalid page_id")
            }
        }

        /* Add the value to a new page if all existing pages are full */
        if value_id.slot_id.is_none()
        {
            /* Create a page whose page_id will be the number of existing pages in the heapfile */
            let mut page = Page::new(num_pages);

            /* Try adding the value into this new page */
            match page.add_value(&value.clone())
            {
                /* add_value succeeded: update the value_id and write new page to file*/
                Some(slot_id) => {value_id.page_id = Some(num_pages);
                                  value_id.slot_id = Some(slot_id);
                                  match heapfile.write_page_to_file(page)
                                  {
                                      Ok(()) => (),
                                      Err(error) => panic!("insert_value: could not write page to file")
                                  };
                                 },

                /* add_value failed: we should never fail when adding a value to a new page */
                None => panic!("insert_value: Failed to add value to new page")
            }
        }

        /* Return ValueId information once we have successfully inserted the new value */
        return value_id;
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        /* Initialize a new vector that will hold the ValueIds to be returned */
        let mut value_ids = Vec::new();

        /* Insert each value in the argument vector into the heapfile */
        for i in 0..values.len()
        {
            /* Insert ith value and append the returned ValueId to the result vector */
            value_ids.push(self.insert_value(container_id, values[i].clone(), tid));
        }

        /* Return the vector of returned value_ids */
        return value_ids;
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {

        /* Get the heapfile with the provided container_id */
        let heapfiles = self.heapfiles_lock.clone().write().unwrap().clone();
        let heapfile = self.lookup_hf(heapfiles, id.container_id).clone();

        /* Make sure that neither page_id nor slot_id are None values */
        if id.page_id.is_none() || id.slot_id.is_none()
        {
            return Err(CrustyError::CrustyError(String::from("ERROR")))
        }

        /* Try to read the page from file with the provided page_id */
        match heapfile.read_page_from_file(id.page_id.unwrap())
        {
            Ok(mut page) =>
                /* Try to delete the value from the page */
                match page.delete_value(id.slot_id.unwrap())
                {
                    Some(()) => return Ok(()),
                    None => Ok(())
                },
            Err(error) => Ok(())
        }
    }

    /// Updates a value. Returns record ID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        /* Try to delete a value */
        match self.delete_value(id, _tid)
        {
            /* delete_value succeeded: insert the new value into the heapfile */
            Ok(()) => return Ok(self.insert_value(id.container_id, value, _tid)),

            /* delete_value failed: return a CrustyError */
            Err(error) => return Err(error)
        }
    }

    /// Create a new container to be stored.
    fn create_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        /* Get RwLock to append a heapfile onto the heapfiles vector */
        {
            /* Get a pointer to the heapfiles vector */
            let mut heapfiles = self.heapfiles_lock.write().unwrap();

            /* Create a new file_path based on the length of the heapfiles vector */
            let mut file_path = PathBuf::new();
            let string = format!("{}{}", self.storage_path, heapfiles.len());
            file_path.push(string);

            let heapfile_result = HeapFile::new(file_path, container_id);

            match heapfile_result
            {
                Ok(heapfile) => heapfiles.push(Arc::new(heapfile)),
                Err(error) => return Err(error)
            }
        }
        return Ok(());
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        /* Get a pointer to the heapfiles vector */
        let mut heapfiles = self.heapfiles_lock.write().unwrap();

        /* Loop through the heapfiles vector */
        for i in 0..heapfiles.len(){

            /* Return the heapfile whose ContainerId matches container_id */
            if HeapFile::container_id(&heapfiles[i]) == container_id
            {
                heapfiles.remove(i);
                return Ok(());
            }
        }
        return Err(CrustyError::CrustyError(String::from("ERROR")))
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        /* Get the heapfile pointer given the provided container_id */
        let heapfiles = self.heapfiles_lock.clone().write().unwrap().clone();
        let heapfile = self.lookup_hf(heapfiles, container_id).clone();

        /* Return a new iterator */
        return HeapFileIterator::new(container_id, tid, heapfile);
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        /* Get the heapfile pointer given the provided container_id */
        let heapfiles = self.heapfiles_lock.clone().read().unwrap().clone();
        let heapfile = self.lookup_hf(heapfiles, id.container_id).clone();

        /* Make sure that neither page_id nor slot_id are None values */
        if id.page_id.is_none() || id.slot_id.is_none()
        {
            return Err(CrustyError::CrustyError(String::from("get_value: ValidId invalid")))
        }

        /* Try to read a page from heapfile given the page_id */
        match heapfile.read_page_from_file(id.page_id.unwrap())
        {
            /* read_page_from_file succeeded */
            Ok(page) =>
                match page.get_value(id.slot_id.unwrap())
                {
                    Some(value) => return Ok(value),
                    None => return Err(CrustyError::CrustyError(String::from("get_value: could not get value from page")))
                },

            /* read_page_from_file failed */
            Err(error) => return Err(CrustyError::CrustyError(String::from("get_value: could not read page from file")))
        }
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager.
    /// If there is a buffer pool it should be reset.
    fn reset(&self) {
        panic!("TODO milestone hs");
    }

    /// Shutdown the storage manager. Can call drop. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    fn shutdown(&self) {

        drop(self);

        if self.is_temp == true
        {
            let heapfiles = self.heapfiles_lock.write().unwrap();
            for i in 0..heapfiles.len()
            {
                let string = format!("{}{}", self.storage_path, i);
                match fs::remove_file(string)
                {
                    Ok(()) => (),
                    Err(error) => panic!("shutdown: could not remove temporary file")
                }
            }
        }
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    /// Shutdown the storage manager. Can call be called by shutdown. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    fn drop(&mut self) {
        //panic!("TODO milestone hs"); TODO remove this later
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_container(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.get_bytes()[..], p2.get_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_container(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = Vec::new();
        byte_vec.push(get_random_byte_vec(400));
        byte_vec.push(get_random_byte_vec(400));
        byte_vec.push(get_random_byte_vec(400));
        for val in &byte_vec {
            {sm.insert_value(cid, val.clone(), tid);}
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        let mut byte_vec2: Vec<Vec<u8>> = Vec::new();
        // Should be on two pages
        byte_vec2.push(get_random_byte_vec(400));
        byte_vec2.push(get_random_byte_vec(400));
        byte_vec2.push(get_random_byte_vec(400));
        byte_vec2.push(get_random_byte_vec(400));

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        let mut byte_vec2: Vec<Vec<u8>> = Vec::new();
        // Should be on 3 pages
        byte_vec2.push(get_random_byte_vec(300));
        byte_vec2.push(get_random_byte_vec(500));
        byte_vec2.push(get_random_byte_vec(400));

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_container(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
