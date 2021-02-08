import os
import time
import hashlib

from cassandra.cluster import Cluster
from tqdm import tqdm


def read_by_chunks(fname, chunk = 1048576):
    """Generator to ready file by file chunks

    :fname: filename

    :chunk: size of chunk in bytes

    """
    with open(fname, 'rb') as f:
        data = f.read(chunk)
        while data:
            yield data
            data = f.read(chunk)


def write_by_chunks(generator, ofn):
    """Write output file chunk generator to a file

    :generator: generator that yield byte chunks

    :ofn: output filename

    """
    with open(ofn, 'wb') as f:
        for data in generator:
            f.write(data)


def _hash(key):
    h = hashlib.sha512()

    if isinstance(key, bytes):
        h.update(key)
        return h.hexdigest()

    if not isinstance(key, (tuple, list)):
        h.update(str(key).encode('utf-8'))
        return h.hexdigest()

    for x in key:
        h.update(_hash(x).encode('utf-8'))
    return h.hexdigest()


def touch(fname, size = 10485760):
    with open(fname, 'wb') as f:
        f.seek(size - 1)
        f.write(b'\0')


def touch_random(fname, size = 10485760):
    with open(fname, 'wb') as f:
        f.write(os.urandom(size))


class Cassandra_Files:

    def __init__(self, cluster_ips, keyspace_suffix=''):
        """
        :keyspace_suffix: suffix of the keyspace

        :cluster_ips: cluster ips

        """
        self._cluster_ips = cluster_ips
        self._cluster = Cluster(cluster_ips)

        keyspace_name = 'cassandra_files_' + keyspace_suffix
        self._init_keyspace(keyspace_name = keyspace_name)
        self._session = self._cluster.connect(keyspace_name)
        self._init_tables()
        self._queries = self._prepare_queries()


    def _init_keyspace(self, keyspace_name):
        session = self._cluster.connect()
        print('CREATE KEYSPACE IF NOT EXISTS %s' % keyspace_name)
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS
        %s
        WITH REPLICATION = {
        'class' : 'NetworkTopologyStrategy',
        'datacenter1' : 1}""" % keyspace_name)


    def _init_tables(self):
        create_queries = [
            """
            CREATE TABLE IF NOT EXISTS
            files
            (
            filename text,
            timestamp text,
            chunk_order int,
            chunk_id text,
            PRIMARY KEY (filename, timestamp, chunk_order)
            )""",
            """
            CREATE TABLE IF NOT EXISTS
            files_inode
            (
            chunk_id text,
            chunk blob,
            PRIMARY KEY(chunk_id)
            )"""]


        for query in create_queries:
            self._session.execute(query)


    def _prepare_queries(self):
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
        res['select_max_timestamp'] = self._session.prepare\
            ("""
            SELECT max(timestamp)
            FROM files
            WHERE filename=?""")
        res['select_chunk_id'] = self._session.prepare\
            ("""
            SELECT chunk_id
            FROM files
            WHERE filename=? and timestamp=?""")
        res['select_chunk'] = self._session.prepare\
            ("""
            SELECT chunk
            FROM files_inode
            WHERE chunk_id=?""")
        return res


    def _get_file_chunks(self, filename):
        timestamp = self._session.execute\
            (self._queries['select_max_timestamp'],
             [filename]).one()[0]

        if timestamp is None:
            raise RuntimeError("%s file does not exists!" \
                               % filename)

        chunk_order = self._session.execute\
            (self._queries['select_chunk_id'],
             [filename, timestamp])

        for chunk_id in chunk_order:
            yield self._session.execute\
                (self._queries['select_chunk'],
                 [chunk_id[0]])\
                 .one()[0]


    def download(self, cassandra_fn, ofn):
        write_by_chunks(self._get_file_chunks(cassandra_fn),
                        ofn = ofn)


    def upload_file(self, fname, cassandra_fn):
        timestamp = str(time.time())

        for chunk_order, data in enumerate(read_by_chunks(fname)):
            # hashing timestamp and filename prevents problems with
            # files deleting. however, this does not allow
            # deduplication, e.g. two identical files will occupy
            # double the size. alternatively, one can keep number of
            # 'links' for every chunk_id, however this solution
            # involves counters, and they can be buggy in cassandra.
            chunk_id = _hash((data, timestamp, cassandra_fn))
            self._session.execute(self._queries['insert_files'],
                                  (cassandra_fn, timestamp,
                                   chunk_order, chunk_id))
            self._session.execute(self._queries['insert_files_inode'],
                                  (chunk_id, data))


cfs = Cassandra_Files(cluster_ips = ['172.17.0.2'])
