# File storage and spatial index with Apache Cassandra

This implements a file storage and a spatial index with Apache
Cassandra.

Files are stored in chunks. Simultaneous writes are allowed, with
several version of a file are timestamped. Only the most recent file
is downloaded. The file storage is intended for a storage of files
that are only generated once (and only by one process).

The spatial indexing storage is based on the
(geohash)[https://en.wikipedia.org/wiki/Geohash]. The spatial index
allows to search for polygon regions.
