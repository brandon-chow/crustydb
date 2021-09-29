# BufferPool Milestone (bp)

In this milestone you will be building a buffer pool for pages in your heapstore storage manager. This milestone is more open-ended than hs or qo as we do not even provide you with a template/trait or skeleton code. It is your job to design how a buffer pool should interact with your storage manager. 

- Your buffer pool should only hold a limited number of pages/frames. This is defined as `PAGE_SLOTS` in common (`use common::PAGE_SLOTS`).
- We *strongly* suggest that your buffer pool return a copy or a smart pointer to the page instead of trying to return a reference to the page (lifetimes here will be tricky due to the SM effectively needing to live for the entire program). I would start with your buffer pool holding the bytes for page (along with metadata), and when the BP serves a request for a page it creates a new page from the bytes.
-  Your buffer pool should implement `FORCE` which will result in any change/update to a page to forcibly be written to the disk/file. An optional 4th milestone could include doing lazy/asynchronous writes.
- Your buffer pool should implement `NO_STEAL` which means any pinned page or dirty page cannot be evicted or written to disk. If your BP needs to evict a page but all are dirty or pinned then it should `panic!()`. For the current milestone this likely will not happen as we are using `FORCE` and there are no pins currently.
- The following function in SM is there to clear out any cached pages in your BP. Use this for testing by having this function clear out the BP.     
```
/// Testing utility to reset all state associated the storage manager.
/// If there is a buffer pool it should be reset.
fn reset(&self) {

```

- Write new tests that start with my_test_bp_
- If you add new files in heapstore remember you need to add them in git and add the mod in lib.rs (or the compiler will ignore them).

## Scoring and Requirements

70% of your score on this milestone is based on correctness that is demonstrated by passing all of the provided unit and integration tests in the HS package. This means when running `cargo test -p heapstore test_bp_` all tests pass. **10% of your milestone is based on new tests that you add to test your code and demonstrate its correctness with.** 10% of your score is based on code quality (following good coding conventions, comments, well organized functions, etc). 10% is based on your write up (my-bp.txt). The write up should contain:
 -  A brief describe of your solution, in particular what design decisions you took and why. This is only needed for part of your solutions that had some significant work (e.g. just returning a counter or a pass through function has no design decision).
- If you had a partner, describing how you split the work. REMEMBER you are both responsible for understanding the code and milestone (CrustyDB questions are fair game on quizzes).
- Briefly describe what use cases your new tests cover. 
- How long you roughly spent on the milestone, and what would have liked/disliked on the milestone.
- If you know some part of the milestone is incomplete, write up what parts are not working, how close you think you are, and what part(s) you got stuck on.
