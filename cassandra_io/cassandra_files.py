import time

from cassandra.cluster import Cluster

from cassandra_io.utils import \
    read_by_chunks, write_by_chunks, \
    write_bytesio_by_chunk, hash_any


class Cassandra_Files:
    """Store files in cassandra storage

    File is split onto chunks and files are stored with
    timestamps. Synchronous write of the same file are allowed, only
    the most recent successfully uploaded version is for downloads.

    Use 'upload' method to upload a local file.

    Use 'delete' to completely delete file from the cassandra storage.

    Use 'download' to download file to a local file, and
    'download_bytesio' to download file to a memory bytes stream.

    Dangling chunks can be removed with 'cleanup' method.

    """


    def __init__(self, cluster_ips, keyspace_suffix='', chunk_size = 1048576):
        """
        :keyspace_suffix: suffix of the keyspace

        :cluster_ips: cluster ips

        :chunk_size: size of chunks to write for files

        """
        self._cluster_ips = cluster_ips
        self._chunk_size = chunk_size
        self._cluster = Cluster(cluster_ips)

        keyspace_name = 'cassandra_files_' + keyspace_suffix
        self._init_keyspace(keyspace_name = keyspace_name)
        self._session = self._cluster.connect(keyspace_name)
        self._queries = self._create_tables_queries()
        for _, query in self._queries.items():
            self._session.execute(query)
        self._queries.update(self._insert_queries())
        self._queries.update(self._select_queries())
        self._queries.update(self._delete_queries())


    def _init_keyspace(self, keyspace_name):
        session = self._cluster.connect()
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS
        %s
        WITH REPLICATION = {
        'class' : 'NetworkTopologyStrategy',
        'datacenter1' : 1}""" % keyspace_name)


    def _create_tables_queries(self):
        res = {}
        res['create_files'] = """
            CREATE TABLE IF NOT EXISTS
            files
            (
            filename text,
            timestamp text,
            chunk_order int,
            chunk_id text,
            PRIMARY KEY (filename, timestamp, chunk_order))"""
        res['create_files_inode'] = """
            CREATE TABLE IF NOT EXISTS
            files_inode
            (
            chunk_id text,
            chunk blob,
            PRIMARY KEY(chunk_id))"""
        res['create_files_timestamp'] = """
            CREATE TABLE IF NOT EXISTS
            files_timestamp
            (
            filename text,
            timestamp text,
            PRIMARY KEY(filename))"""

        return res


    def _insert_queries(self):
        res = {}
        res['insert_files'] = """
            INSERT INTO files
            (filename, timestamp, chunk_order, chunk_id)
            VALUES (%s, %s, %s, %s)
            IF NOT EXISTS"""
        res['insert_files_inode'] = """
            INSERT INTO files_inode
            (chunk_id, chunk)
            VALUES (%s, %s)
            IF NOT EXISTS"""
        res['insert_files_timestamp'] = """
            INSERT INTO files_timestamp
            (filename, timestamp)
            VALUES (%s, %s)"""

        return res


    def _delete_queries(self):
        res = {}
        res['delete_from_files_inode'] = \
            self._session.prepare\
            ("""
            DELETE FROM files_inode
            WHERE chunk_id=?
            IF EXISTS""")
        res['delete_from_files'] = \
            self._session.prepare\
            ("""
            DELETE FROM files
            WHERE timestamp=?
            and filename=?
            and chunk_order=?
            IF EXISTS""")
        res['delete_from_files_timestamp'] = \
            self._session.prepare\
            ("""
            DELETE FROM files_timestamp
            WHERE filename=?
            IF EXISTS""")

        return res


    def _select_queries(self):
        res = {}
        res['select_current_timestamp'] = \
            self._session.prepare\
            ("""
            SELECT timestamp
            FROM files_timestamp
            WHERE filename=?""")
        res['select_chunk_id'] = \
            self._session.prepare\
            ("""
            SELECT chunk_id
            FROM files
            WHERE filename=? and timestamp=?""")
        res['select_chunk'] = \
            self._session.prepare\
            ("""
            SELECT chunk
            FROM files_inode
            WHERE chunk_id=?""")
        res['select_current_filenames'] = \
            self._session.prepare\
            ("""
            SELECT filename, timestamp
            FROM files_timestamp
            """)
        res['select_all_chunks'] = \
            self._session.prepare\
            ("""
            SELECT filename, timestamp, chunk_order, chunk_id
            FROM files
            WHERE
            filename=?""")
        res['select_older_chunks'] = \
            self._session.prepare\
            ("""
            SELECT filename, timestamp, chunk_order, chunk_id
            FROM files
            WHERE
            filename=? and timestamp<?""")
        return res


    def _get_file_chunks(self, filename):
        timestamp = self._session.execute\
            (self._queries['select_current_timestamp'],
             [filename]).one()

        if timestamp is None:
            raise RuntimeError("%s file does not exists!" \
                               % filename)

        timestamp = timestamp[0]
        chunk_order = self._session.execute\
            (self._queries['select_chunk_id'],
             [filename, timestamp])

        for chunk_id in chunk_order:
            yield self._session.execute\
                (self._queries['select_chunk'],
                 [chunk_id[0]])\
                 .one()[0]


    def _delete(self, files):
        for filename, timestamp, chunk_order, chunk_id in files:
            self._session.execute\
                (self._queries['delete_from_files'],
                 [timestamp, filename, chunk_order])
            self._session.execute\
                (self._queries['delete_from_files_inode'],
                 [chunk_id])


    def download(self, cassandra_fn, ofn):
        """Download a file from cassandra storage and save it locally

        :cassandra_fn: filename in the cassandra storage

        :ofn: output file

        :return: nothing
        """
        write_by_chunks(self._get_file_chunks(cassandra_fn),
                        ofn = ofn)


    def download_bytesio(self, cassandra_fn):
        """Download a file from cassandra storage into memory

        :cassandra_fn: filename in the cassandra storage

        :return: BytesIO stream
        """
        return write_bytesio_by_chunk\
            (self._get_file_chunks(cassandra_fn))


    def cleanup(self):
        """Delete versions of file in the storage older than the current one

        """
        most_recent = self._session.execute\
            (self._queries['select_current_filenames'])

        for filename, timestamp in most_recent:
            files = self._session.execute\
                (self._queries['select_older_chunks'],
                 [filename, timestamp])
            self._delete(files)


    def delete(self, cassandra_fn):
        """Delete file from the cassandra storage completely

        :cassandra_fn: filename in the cassandra storage

        """
        self._session.execute\
            (self._queries['delete_from_files_timestamp'],
             [cassandra_fn])

        chunks = self._session.execute\
            (self._queries['select_all_chunks'],
             [cassandra_fn])
        self._delete(chunks)


    def upload(self, ifn, cassandra_fn):
        """Upload file to the cassandra storage

        :ifn: path to the local filename

        :cassandra_fn: filename in the cassandra storage

        """
        timestamp = str(time.time())

        for chunk_order, data in enumerate\
            (read_by_chunks(ifn, self._chunk_size)):
            # hashing timestamp and filename prevents problems with
            # files deleting. however, this does not allow
            # deduplication, e.g. two identical files will occupy
            # double the size. alternatively, one can keep number of
            # 'links' for every chunk_id, however this solution
            # involves counters, and they can be buggy in cassandra.
            chunk_id = hash_any((cassandra_fn,
                                 timestamp, data))
            self._session.execute\
                (self._queries['insert_files'],
                 (cassandra_fn, timestamp,
                  chunk_order, chunk_id))
            self._session.execute\
                (self._queries['insert_files_inode'],
                 (chunk_id, data))

        self._session.execute\
            (self._queries['insert_files_timestamp'],
             (cassandra_fn, timestamp))