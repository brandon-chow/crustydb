use crate::page::Page;
use common::ids::{ContainerId, PageId};
use common::{CrustyError, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use std::io::BufWriter;
use std::io::{Seek, SeekFrom};
use std::mem;

/// The struct for a heap file.
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
pub(crate) struct HeapFile {
    pub container_id_lock: RwLock<ContainerId>,
    pub file_lock: RwLock<File>,
    pub num_pages_lock: RwLock<PageId>,

    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path and container Id. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {

        /* Initialize a file object given file_path */
        let file = match OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&file_path)
                    {
                        Ok(f) => f,
                        Err(error) => {panic!("Could not open file")} /* TODO */
                    };

        /* Create a rwlock for the file struct */
        let file_lock = RwLock::new(file);

        let container_id_lock = RwLock::new(container_id);

        /* Initialize num_pages to zero */
        let num_pages = 0;

        /* Create a rwlock for the num_pages field */
        let num_pages_lock = RwLock::new(num_pages);

        Ok(HeapFile {
            container_id_lock: container_id_lock,
            file_lock: file_lock,
            num_pages_lock: num_pages_lock,
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }

    pub fn container_id(&self) -> ContainerId {
        let container_id: ContainerId = *self.container_id_lock.read().unwrap();
        return container_id;
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        let ans: PageId;
        {
            /* Get a read lock */
            let num_pages = self.num_pages_lock.read().unwrap();
            ans = *num_pages;
        } /* Read lock gets dropped */
        return ans;
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }

        /* Create an empty page */
        let new_page: Page;

        {
            /* Get a write lock for file */
            let mut file = self.file_lock.write().unwrap();

            /* Check that pid is valid */
            if pid as usize > self.num_pages() as usize - 1
            {
                return Err(CrustyError::CrustyError(String::from("THIS IS MY ERROR")));
            }

            /* Seek to the page */
            file.seek(SeekFrom::Start(mem::size_of::<ContainerId>() as u64 + (pid as u64) * (PAGE_SIZE as u64)))?;

            /* Create a buffer */
            let mut buffer = [0; PAGE_SIZE];

            /* Read PAGE_SIZE bytes into the buffer */
            file.read(&mut buffer)?;

            /* Turn array of bytes into a page */
            new_page = Page::from_bytes(&buffer);
        }
        /* Write lock is dropped here */

        /* Return the page */
        return Ok(new_page);
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }

        /* Create an empty vector */
        let mut vec = Vec::new();

        /* Convert the page to a vector of bytes */
        vec = Page::get_bytes(&page);

        //let b = &vec;
        let c: &[u8] = &vec;

        /* Increment num_pages by one if this is a new page*/
        if page.get_page_id() >= self.num_pages()
        {
            /* Get a write lock */
            let mut num_pages = self.num_pages_lock.write().unwrap();
            *num_pages += 1;
        } /* Write lock dropped */

        {
            /* Get a write lock */
            let mut file = self.file_lock.write().unwrap();

            /* Seek to the page */
            file.seek(SeekFrom::Start(mem::size_of::<ContainerId>() as u64 + (page.get_page_id() as u64) * (PAGE_SIZE as u64)))?;

            file.write(c);

        } /* Write lock dropped */

        return Ok(());
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf(), 1).unwrap();

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.get_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.get_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.get_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
