import json
import hashlib
import geohash
from shapely import geometry

from cassandra import query

from cassandra_io.base import Cassandra_Base
from cassandra_io.utils import bbox2hash, bboxes2hash

from cassandra_io.polygon_index import \
    Polygon_File_Index


def hash_string(string):
    h = hashlib.md5()
    h.update(string.encode('utf-8'))
    return h.hexdigest()


def _chunker(seq, size):
    return (seq[pos:pos + size] \
            for pos in range(0, len(seq), size))


class Cassandra_Spatial_Index(Cassandra_Base):
    """Store index with a geohash tables



    """

    def __init__(self, hash_min = 2, depth = 3, delta = 1.5, **kwargs):
        """Init

        :hash_min, depth: defines a range of lengths used hash
        strings. hash_max := hash_min + depth

        :delta: determines how larger the geohash box should be when
        splitting data onto subgeohash

        :kwargs: arguments passed to Cassandra_Base

        """
        self._hash_min = int(hash_min)
        self._hash_max = int(hash_min + depth)
        self._delta = delta
        self._datahash_length = len(hash_string(""))
        self._geohash_accuracy = \
            [geohash.decode_exactly('0'*x)[2:] \
             for x in range(self._hash_min,
                            self._hash_max + 1)]

        if self._datahash_length <= self._hash_max and \
           self._datahash_length >= self._hash_min:
            raise RuntimeError("""
            Data hash string length coincides with the geohash.
            Cannot proceed!""")

        if 'keyspace' not in kwargs:
            kwargs['keyspace'] = 'cassandra_spatial_index'
        kwargs['keyspace'] += '_hash_lengths_%d_%d' \
            % (self._hash_min, self._hash_max)
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

        for i in range(self._hash_min, self._hash_max + 1):
            res['create_hash%d' % i] = """
            CREATE TABLE IF NOT EXISTS
            hash%d
            (
            hash text,
            data_id text,
            PRIMARY KEY(hash, data_id))""" % i

        return res


    def _insert_queries(self):
        res = {}
        res['insert_data'] = """
        INSERT INTO data
        (data_id, data)
        VALUES (%s, %s)
        IF NOT EXISTS"""

        for i in range(self._hash_min, self._hash_max + 1):
            res['insert_hash%d' % i] = \
                'INSERT INTO hash%d ' % i + \
                """
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
            WHERE data_id in ?""")

        for i in range(self._hash_min, self._hash_max + 1):
            res['select_hash%d' % i] = \
                self._session.prepare\
                ("""
                SELECT data_id
                FROM hash%d
                WHERE hash in ?""" % i)

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
             [[data_id]]).one()[0]
        return json.loads(data)


    def _load_many(self, data_ids):
        data = self._session.execute\
            (self._queries['select_data'],
             [data_ids])
        return [json.loads(x[0]) for x in data]


    def _polygon2bbox(self, polygon, lon_first):
        bbox = geometry.Polygon(polygon).bounds

        if lon_first:
            bbox = [bbox[1],bbox[0],bbox[3],bbox[2]]

        return bbox


    def _hashbbox_compare(self, cur_hash, bbox):
        i = cur_hash - self._hash_min
        glat, glon = self._geohash_accuracy[i]

        blat = abs(bbox[2] - bbox[0])
        blon = abs(bbox[3] - bbox[1])

        return max(glat / blat, glon / blon) > self._delta


    def _insert_hash(self, bbox):
        cur_hash = self._hash_min
        hashes = bbox2hash(bbox, cur_hash)

        while self._hashbbox_compare(cur_hash, bbox) \
              and cur_hash < self._hash_max:
            hashes = bbox2hash(bbox, cur_hash + 1)

            for d in hashes:
                self._session.execute\
                    (self._queries['insert_hash%d' % cur_hash],
                     [d[:-1], d])
            cur_hash += 1

        return cur_hash, hashes


    def insert(self, data, lon_first = True):
        data_s = json.dumps(data)
        data_id = hash_string(data_s)

        if self._session.execute\
           (self._queries['select_anydata'],
            [data_id]).one() is not None:
            return

        bbox = self._polygon2bbox(data['polygon'],
                                  lon_first)
        hash_len, hashes = self._insert_hash(bbox)

        for h in hashes:
            self._session.execute\
                (self._queries['insert_hash%d' % hash_len],
                 [h, data_id])

        self._session.execute\
            (self._queries['insert_data'],
             [data_id, data_s])


    def _query_bbox(self, bbox_list):
        """Query data_ids from the Cassandra Spatial Index

        :bbox_list: list of bounding box of the query

        :return: a list of data_ids that *might have* a non-zero
        intersection with a desired bbox

        """
        cur_hash = self._hash_min
        hashes = bboxes2hash(bbox_list, cur_hash)
        data_ids = []

        while len(hashes):
            hashes = set(hashes)\
                .intersection(bboxes2hash(bbox_list, cur_hash))
            q_res = self._session.execute\
                (self._queries['select_hash%d' % cur_hash],
                 query.ValueSequence\
                 ([hashes]))

            hashes = []
            for x in q_res:
                x = x[0]
                if len(x) == self._datahash_length:
                    data_ids += [x]
                else:
                    hashes += [x]
            cur_hash += 1

        return data_ids


    def intersect(self, polygons, lon_first = True,
                  chunk_size = 2**15):
        """Produce an index that has an intersection with a given polygons

        :polygons: either a list of coordinate tuples, or a
        geometry.Polygon or geometry.MultiPolygon

        :chunk_size: number of data entries to query per
        iteration. One spatial index entry is about 500 bytes.

        """
        if not isinstance(polygons, geometry.multipolygon.MultiPolygon):
            if isinstance(polygons, geometry.polygon.Polygon):
                polygons = geometry.multipolygon.MultiPolygon([polygons])
            elif isinstance(polygons[0], tuple):
                polygons = geometry.multipolygon.MultiPolygon\
                    ([geometry.polygon.Polygon(polygons)])
            else:
                polygons = geometry.multipolygon.MultiPolygon\
                    ([geometry.polygon.Polygon(x) for x in polygons])

        bboxes = [self._polygon2bbox(pl, lon_first) for pl in polygons]
        data_ids = list(set(self._query_bbox(bboxes)))

        index = Polygon_File_Index()
        for data_chunk in _chunker(data_ids, size = chunk_size):
            datas = self._load_many(data_chunk)

            for data in datas:
                # ignore data that has no intersection
                if not polygons.intersects\
                   (geometry.polygon.Polygon(data['polygon'])):
                    continue

                index.insert(data)

        return index
