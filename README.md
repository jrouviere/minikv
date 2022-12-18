# MiniKV

A naive toy implementation of a key value store based on LSM trees / SSTable.

The project is following the naming convention from Apache Cassandra and various other databases. Although the principles are the same the structures and file format used have been greatly simplified in that implementation:

- memtable: data store, in memory
- commit log or WAL: append only file for recovery, on disk
- SSTable: sorted string table, on disk
