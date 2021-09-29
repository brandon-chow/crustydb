use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
use std::convert::TryInto;
use std::mem;

/// The struct for a page. Note this can hold more elements/meta data when created,
/// but it must be able to be packed/serialized/marshalled into the data array of size
/// PAGE_SIZE. In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// You do not need reclaim header information for a value inserted (eg 6 bytes per value ever inserted)
/// The rest must filled as much as possible to hold values.
pub(crate) struct Page {
    /// The data for data
    page_id : PageId,
    header_len : u16,
    header : Vec<HeaderTupleT>,
    data : [u8; PAGE_SIZE],
}



struct HeaderTuple {
    slot_id : SlotId,
    index : u16,
    length : u16,
}
type HeaderTupleT = HeaderTuple;

/// The functions required for page
impl Page {
    /// Create a new page
    pub fn new(page_id: PageId) -> Self {
        let page = Page {
            page_id,
            header_len : 0,
            header : Vec::new(),
            data : [0; PAGE_SIZE]
        };
        return page;
    }

    /// Return the page id for a page
    pub fn get_page_id(&self) -> PageId {
        return self.page_id;
    }

    /// find_min_available_slot_id: This function returns the minimum available SlotId
    ///
    /// header: The header vector for our page
    ///
    /// returns: The minimum available SlotId
    pub fn find_min_available_slot_id(&mut self) -> SlotId
    {
        /* Loop through header vector to check if i is a used SlotId */

        for i in 0..self.header.len()
        {
            let mut is_used = false; /* isUsed tracks if i is already used */

            /* Loop again to check whether i matches against any SlotId in the header vector */
            for j in 0..self.header.len()
            {
                if self.header[j].slot_id as usize == i
                {
                    is_used = true;
                }
            }

            /* If is_used is true, we return i */
            if is_used == false
            {
                return i as u16;
            }
        }
        /* Otherwise, return one greater than the number of SlotId's already in use */
        return self.header.len() as u16;
    }


    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you can replace the slotId.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    pub fn add_value(&mut self, bytes: &Vec<u8>) -> Option<SlotId> {

        let mut num = 0; /* We are adding a new tuple in the nth position of the header vector */
        let mut index: usize = 0; /* This is the index of the value we are adding */
        let mut space_is_found = false; /* Represent whether we found free space */

        /* Loop through header vector to find continguous free space to fit the bytes vector */
        if self.header.len() > 1
        {
            for i in 0..self.header.len() - 1
            {
                /* Set temp_index to the end of a indexed piece of data */
                let temp_index: usize = self.header[i].index as usize + self.header[i].length as usize;

                /* Calculate the number of bytes of free space between the end of header[i].index and the start of header[i + 1].index */
                let free_space: usize = self.header[i + 1].index as usize - temp_index;

                /* If the amount of free_space is enough to fit bytes, then we break and assign temp_index to index */
                if free_space >= bytes.len()
                {
                    index = temp_index;
                    space_is_found = true;
                    num = i;
                    break;
                }
            }
        }

        /* Find the maximum number of bytes that our data array can hold */
        let max = PAGE_SIZE - 8 - 6 * self.header.len();

        /* Return None if no index was found */
        if space_is_found == false
        {
            if self.header.len() > 0
            {
                let temp_index: usize = self.header[self.header.len() - 1].index as usize + self.header[self.header.len() - 1].length as usize;
                if max - temp_index < bytes.len()
                {
                    return None;
                }
                index = temp_index;
                num = self.header.len();
            }
            /* This will be the first value inserted in the data array */
            else
            {
                /* Insert a value into data[0] */
                index = 0;
                num = 0;
            }
        }

        /* Find the minimum available SlotId for this new value */
        let slot_id: SlotId = Self::find_min_available_slot_id(self);

        /* Create a new tuple that represents the new value we're adding to the data array */
        let header_entry: HeaderTupleT = HeaderTuple {slot_id: slot_id, index: index as u16, length: bytes.len() as u16};

        /* Insert a new tuple into the header vector at the ith position */
        self.header.insert(num, header_entry);

        self.header_len += 1;

        /* Clone the bytes into the page's data array */
        self.data[index..index + bytes.len()].clone_from_slice(&bytes);

        /* Return the SlotId of the new value */
        return Some(slot_id);
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {

        /* The index of the slice we are returning */
        let index: usize;

        /* Find the tuple in our header that matches the slot_id */
        for i in 0..self.header.len()
        {
            /* Check if header[i].slot_id matches slot_id */
            if self.header[i].slot_id == slot_id
            {
                index = self.header[i].index as usize;

                /* Return a vector of that slice if we have found the right slot_id */
                return Some(self.data[index..index+self.header[i].length as usize].to_vec());
            }
        }

        /* Return None if the slot_id is not found */
        return None;
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// HINT: Return Some(()) for a valid delete
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {

        /* Find the tuple in our header that matches the slot_id */
        for i in 0..self.header.len()
        {
            /* Check if header[i].slot_id matches slot_id */
            if self.header[i].slot_id == slot_id
            {
                /* Remove this tuple from our header vector */
                self.header.remove(i);
                return Some(());
            }
        }

        /* Return None if the slot_id is not found */
        return None;
    }

    /// Create a new page from the byte array.
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    pub fn from_bytes(data: &[u8]) -> Self {

        /* Get the page_id from the byte array */
        let page_id: PageId = PageId::from_le_bytes(data[0..mem::size_of::<PageId>()].try_into().unwrap());

        /* Get the header_len from the byte array */
        let header_len: u16 = u16::from_le_bytes(data[mem::size_of::<PageId>()..mem::size_of::<PageId>() + 2].try_into().unwrap());

        /* Create a new empty page struct given the page_id we just parsed */

        let mut new_page = Page {
            page_id: page_id,
            header_len: header_len,
            header: Vec::new(),
            data: [0; PAGE_SIZE]
        };

        for i in 0..header_len as usize
        {
            /* Position of the start of the slot field */
            let slot_pos: usize = 6 * i + 8;

            /* Position of the start of the index field */
            let index_pos: usize = slot_pos + mem::size_of::<SlotId>();

            /* Position of the start of the length field */
            let length_pos: usize = index_pos + 2;

            /* Fill fields with respective values from the bytes array */
            let slot_id: SlotId = SlotId::from_le_bytes(data[slot_pos..index_pos].try_into().unwrap());
            let index: u16 = u16::from_le_bytes(data[index_pos..length_pos].try_into().unwrap());
            let length: u16 = u16::from_le_bytes(data[length_pos..length_pos + 2].try_into().unwrap());

            /* Create a new HeaderTupleT */
            let header_entry = HeaderTupleT {
                slot_id: slot_id,
                index: index,
                length: length,
            };

            /* Append the newly created header_entry to the end of our header vector */
            new_page.header.push(header_entry);
        }

        /* Position of the start of the data array */
        let data_pos: usize = 8 + 6 * header_len as usize;

        /* Copy the values from data into the data array in our newly created page struct */
        for i in 0..data.len() - data_pos
        {
            /* Copying the information in data into new_page.data byte by byte */
            new_page.data[i..i+1].clone_from_slice(&data[data_pos + i..data_pos + i + 1]);
        }

        /* Return the newly created page struct */
        return new_page;
    }

    /// Convert a page into bytes. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    pub fn get_bytes(&self) -> Vec<u8> {

        /* Create an empty vector */
        let mut vec = Vec::new();

        /* Create a two-byte array representing the page_id */
        vec.extend(self.page_id.to_le_bytes().iter().cloned());

        /* Create a two-byte array representing the header_len */
        vec.extend(self.header_len.to_le_bytes().iter().cloned());

        /* Pad the header with four bytes of zeros */
        let zero: u32 = 0;
        vec.extend(zero.to_le_bytes().iter().cloned());

        /* Loop through the header array and add slot_id, index, and length into the vector */
        for i in 0..self.header_len as usize
        {
            vec.extend(self.header[i].slot_id.to_le_bytes().iter().cloned());
            vec.extend(self.header[i].index.to_le_bytes().iter().cloned());
            vec.extend(self.header[i].length.to_le_bytes().iter().cloned());
        }

        /* Add the bytes from the data array into our vector */
        let data_array_max_size = PAGE_SIZE - 8 - 6 * self.header.len();
        vec.extend(self.data[0..data_array_max_size].iter().cloned());

        /* Return the newly created vector */
        return vec;

    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        return self.header.len() as usize * 6 + 8;
    }

    /// A utility function to determine the largest block of free space in the page.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_largest_free_contiguous_space(&self) -> usize {

        /* Max represents the largest number of bytes of contiguous free space in the page */
        let mut max: usize = 0;

        /* Find contiguous free space within the already allocated bytes in our data array  */
        if self.header.len() > 1
        {
            for i in 0..self.header.len() - 1
            {
                /* Set temp_index to the end of a indexed piece of data */
                let temp_index: usize = self.header[i].index as usize + self.header[i].length as usize;

                /* Calculate the number of bytes of free space between the end of header[i].index and the start of header[i + 1].index */
                let free_space: usize = self.header[i + 1].index as usize - temp_index;

                /* If the amount of free_space is larger than max, set max to free_space */
                if free_space >= max
                {
                    max = free_space;
                }
            }
        }

        /* Maximum number of bytes that our data array can hold */
        let data_array_max_size = PAGE_SIZE - 8 - 6 * self.header.len();

        /* Check if max is less than the free space after the last allocated byte in our data array */
        if self.header.len() == 0
        {
            max = data_array_max_size;
        }
        else
        {
            let final_allocated_index: usize = self.header[self.header.len() - 1].index as usize
                                            + self.header[self.header.len() - 1].length as usize;

            let remaining_free_space: usize = data_array_max_size - final_allocated_index;
            if max < remaining_free_space
            {
                max = remaining_free_space;
            }
        }

        /* Return the largest number of bytes of continguous free space */
        return max;
    }
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
/// See https://stackoverflow.com/questions/30218886/how-to-implement-iterator-and-intoiterator-for-a-simple-struct
pub struct PageIter {
    page: Page,
    index: usize,
}

/// The implementation of the (consuming) page iterator.
impl Iterator for PageIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.page.header.len()
        {
            return None;
        }
        let index = self.page.header[self.index].index as usize;
        let length = self.page.header[self.index].length as usize;
        let valid_value = self.page.data[index..index+length].to_vec();
        self.index += 1;
        return Some(valid_value);
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = Vec<u8>;
    type IntoIter = PageIter;

    fn into_iter(self) -> Self::IntoIter {
        PageIter {
            page: self,
            index: 0,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;


    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(
            PAGE_SIZE - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
    }


    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_largest_free_contiguous_space()
        );
    }


    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_largest_free_contiguous_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_largest_free_contiguous_space()
        );
    }


    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }


    #[test]
    fn hs_page_get_first_free_space() {
        init();
        let p = Page::new(0);
        assert_eq!(1,1);

        let _b1 = get_random_byte_vec(100);
        let _b2 = get_random_byte_vec(50);
    }



    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.get_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.get_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.get_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }


    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.get_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes3.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.get_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(None, iter.next());
    }
}
