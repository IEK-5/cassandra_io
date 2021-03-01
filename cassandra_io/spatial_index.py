import json
import geohash
from shapely import geometry

from cassandra import query

from cassandra_io.base import Cassandra_Base
from cassandra_io.utils import bbox2hash

from cassandra_io.polygon_index import \
    Polygon_File_Index, hash_string


class Cassandra_Spatial_Index(Cassandra_Base):
    """Store index with a geohash tables



    """

    def __init__(self, hash_length = 6, **kwargs):
        """

        :hash_length: length of the hash to use. This controls how
        small the smallest unit in the table is. If the region is too
        big, then there are too many elements in the cassandra
        database pointing to the same data. If the region is too small
        then it'll take more time of the local processing. depth=3 and
        min_length=4 means that the smallest unit is about 0.5km^2.

        :kwargs: arguments passed to Cassandra_Base

        """
        self._hash_length = int(hash_length)

        if 'keyspace' not in kwargs:
            kwargs['keyspace'] = 'cassandra_spatial_index'
        kwargs['keyspace'] += '_hash_length%d' % (self._hash_length,)
        super().__init__(**kwargs)
        self._session = self._cluster.connect(self._keyspace)
        queries = self._create_tables_queries()
        for _, query in queries.items():
            self._session.execute(query)
        self._queries.update(queries)
        self._queries.update(self._insert_queries())
        self._queries.update(self._select_queries())
        #self._queries.update(self._delete_queries())


    def _create_tables_queries(self):
        res = {}
        res['create_data'] = """
        CREATE TABLE IF NOT EXISTS
        data
        (
        data_id text,
        data text,
        PRIMARY KEY(data_id))"""
        res['create_hash'] = """
        CREATE TABLE IF NOT EXISTS
        hash
        (
        hash text,
        data_id text,
        PRIMARY KEY(hash, data_id))"""

        return res


    def _insert_queries(self):
        res = {}
        res['insert_data'] = """
        INSERT INTO data
        (data_id, data)
        VALUES (%s, %s)
        IF NOT EXISTS"""
        res['insert_hash'] = """
        INSERT INTO hash
        (hash, data_id)
        VALUES (%s, %s)
        IF NOT EXISTS"""

        return res


    def _select_queries(self):
        res = {}
        res['select_data'] = \
            self._session.prepare\
            ("""
            SELECT data
            FROM data
            WHERE data_id=?""")
        res['select_hash'] = \
            self._session.prepare\
            ("""
            SELECT data_id
            FROM hash
            WHERE hash in ?""")
        res['select_anydata'] = \
            self._session.prepare\
            ("""
            SELECT data_id
            FROM data
            WHERE data_id=?""")

        return res


    def _load(self, data_id):
        data = self._session.execute\
            (self._queries['select_data'],
             [data_id]).one()[0]
        return json.loads(data)


    def _polygon2bbox(self, polygon, lon_first):
        bbox = geometry.Polygon(polygon).bounds

        if lon_first:
            bbox = [bbox[1],bbox[0],bbox[3],bbox[2]]

        return bbox


    def insert(self, data, lon_first = True):
        data_s = json.dumps(data)
        data_id = str(hash_string(data_s))

        bbox = self._polygon2bbox(data['polygon'],
                                  lon_first)
        hashes = bbox2hash(bbox, self._hash_length)

        if self._session.execute\
           (self._queries['select_anydata'],
            [data_id]).one() is not None:
            return

        for h in hashes:
            self._session.execute\
                (self._queries['insert_hash'],
                 [h, data_id])

        self._session.execute\
            (self._queries['insert_data'],
             [data_id, data_s])


    def intersect(self, polygon, lon_first = True):
        bbox = self._polygon2bbox(polygon,
                                  lon_first)
        data_ids = self._session.execute\
            (self._queries['select_hash'],
             query.ValueSequence\
             ([bbox2hash(bbox, self._hash_length)]))

        index = Polygon_File_Index()
        seen = set()
        for data_id in data_ids:
            if data_id[0] in seen:
                continue

            index.insert(self._load(data_id[0]))

        return index.intersect(polygon)
