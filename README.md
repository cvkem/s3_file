# S3-file

In this project we are working on a rust-reader and a rust-writer that both are AWS S3 (Simple Storage Service) compatible. These tools were developed as an experiment to see how to work with large parquet files on S3. As Parquet is used for big-data we often run into cases where the parquet-data is too large to be loaded into memory. 

The reader contains an underlying cache to ensure that the S3-objects are read in in large chunks to limit the number of S3 GET-requests. The size of this buffer is configurable, but it is best to use a size of 10k. The cacche contains 10 cache-block, so in that case a large object can consume 10x10k is 100k of memory.

We intend to also add a writer that uses the multi-part upload functionality of S3 to write objects, such that (Parquet-)objects of many Gigabytes or even Terrabytes can be produced.