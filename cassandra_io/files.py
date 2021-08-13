import logging
import time

from cassandra_io.base import Cassandra_Base

from cassandra_io.utils import \
    read_by_chunks, write_by_chunks, \
    write_bytesio_by_chunk, hash_any


class Cassandra_Files(Cassandra_Base):
    """Store files in cassandra storage

    File is split onto chunks and files are stored with
    timestamps. Synchronous write of the same file are allowed, only
    the most recent successfully uploaded version is for downloads.

    Use 'upload' method to upload a local file.

    Use 'delete' to completely delete file from the cassandra
    storage.

    Use 'download' to download file to a local file, and
    'download_bytesio' to download file to a memory bytes stream.

    Dangling chunks can be removed with 'cleanup' method.

    Note that 'delete' and 'cleanup' will not remove actual physical
    space on cassandra.

    """


    def __init__(self, keyspace_suffix='',
                 chunk_size = 1048576,
                 timeout = 120,
                 **kwargs):
        """
        :keyspace_suffix: suffix of the keyspace

        :chunk_size: size of chunks to write for files

        :timeout: cluster session default_timeout

        :kwargs: arguments passed to Cassandra_Base

        """
        if 'keyspace' not in kwargs:
            kwargs['keyspace'] = 'cassandra_files'
        kwargs['keyspace'] += '_' + keyspace_suffix
        super().__init__(**kwargs)
        self._chunk_size = chunk_size

        self._session = self._cluster.connect(self._keyspace)
        self._session.default_timeout = timeout
        queries = self._create_tables_queries()
        for _, query in queries.items():
            self._session.execute(query)
        self._queries.update(queries)
        self._queries.update(self._insert_queries())
        self._queries.update(self._select_queries())
        self._queries.update(self._delete_queries())


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
        res['select_contains'] = \
            self._session.prepare\
            ("""
            SELECT count(*)
            FROM files_timestamp
            WHERE
            filename=?""")
        res['select_timestamp'] = \
            self._session.prepare\
            ("""
            SELECT timestamp
            FROM files_timestamp
            WHERE
            filename=?""")

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

        # if file is empty no chunks are available
        if not chunk_order:
            return

        for chunk_id in chunk_order:
            try:
                yield self._session.execute\
                    (self._queries['select_chunk'],
                     [chunk_id[0]])\
                     .one()[0]
            except Exception as e:
                logging.error("""
                _get_file_chunks
                ERROR: %s
                filename: %s
                timestamp: %s
                chunk_id: %s
                """ % (str(e),str(filename), str(timestamp), str(chunk_id)))
                raise e


    def _delete(self, files):
        for filename, timestamp, chunk_order, chunk_id in files:
            self._session.execute\
                (self._queries['delete_from_files'],
                 [timestamp, filename, chunk_order])
            self._session.execute\
                (self._queries['delete_from_files_inode'],
                 [chunk_id])


    def __contains__(self, cassandra_fn):
        res = self._session.execute\
            (self._queries['select_contains'],
             [cassandra_fn]).one()[0]

        return res != 0


    def get_timestamp(self, cassandra_fn):
        res = self._session.execute\
            (self._queries['select_timestamp'],
             [cassandra_fn]).one()

        if res is None:
            return res

        return float(res[0])


    def download(self, cassandra_fn, ofn):
        """Download a file from cassandra storage and save it locally

        :cassandra_fn: filename in the cassandra storage

        :ofn: output file

        :return: nothing
        """
        logging.debug("""
        cassandra_io:files.py:download
        cassandra_fn: %s
        ofn: %s
        """ % (cassandra_fn, ofn))
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
