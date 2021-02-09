# Using Apache Cassandra as a distributed file storage

Files are stored in chunks. Simultaneous writes are allowed, with
several version of a file are timestamped. Only the most recent file
is downloaded.
