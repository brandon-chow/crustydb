#[macro_use]
extern crate log;
mod bp_tests;
mod heapfile;
mod heapfileiter;
mod page;
pub mod storage_manager;
pub mod testutil;

pub(crate) const IS_LRU: bool = true;
