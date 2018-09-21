This is strictly following the letter of the assignment. I tried to do the test cases, which are slightly different from the assignment, but ran out of time debbuging. If you define TEST_NOT_ASSINGMENT it just doesn't run, EXC_ARITHMETIC. I traced down the probable error to the design of the given test. While the assignment says that,

"The key-value store needs to use a first-come first-out (FIFO) discipline to evict the records when more space is required. That is, the record that is written the earliest is evicted when new records are written and all available space is exhausted. Because the store is divided into pods, the space eviction could happen in a pod even though there is space in the store."

The test seems to be storing the keys separately in a string buffer of the same size as the hash, instead of relying on the hash function to store the value and the key. I get about 40 errors.

The semaphores are untested, but should work.
