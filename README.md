# MiniKV

A naive toy implementation of a key value store based on LSM trees / SSTable.

I am reusing the Cassandra naming where possible:

- memtable: data store, in memory
- SSTable: sorted string table, on disk
- WAL or commit log: append only file for recovery, on disk

Although the principles are the same the structures and file format used have been greatly simplified in that implementation.
