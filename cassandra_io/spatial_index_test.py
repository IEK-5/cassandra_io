
from cassandra_io.polygon_index import \
    Polygon_File_Index
from cassandra_io.spatial_index import \
    Cassandra_Spatial_Index


def dummy_index_data():
    x = Polygon_File_Index()

    x.insert(data = {'file':'big',
                     'polygon': [(0,0),(0,2),(2,2),(2,0)]})
    x.insert(data = {'file':'one',
                     'polygon': [(0,0),(0,1),(1,1),(1,0)]})
    x.insert(data={'file':'two',
                   'polygon': [(0,0),(0,1),(1,1),(1,0)]})
    x.insert(data={'file':'three',
                   'polygon': [(0.1,0.1),(0.1,1),(1,1),(1,0.1)]})
    x.insert(data={'file':'four',
                   'polygon': [(0.1,0.1),(0.1,0.9),
                               (0.9,0.9),(0.9,0.1)]})
    x.insert(data={'file':'five',
                   'polygon': [(0,0),(0,0.0001),
                               (0.0001,0.0001),(0.0001,0)]})
    x.insert(data={'file':'six',
                   'polygon': [(0,0),(0,0.00001),
                               (0.00001,0.00001),(0.00001,0)]})

    return x


def test_spatial_index(ips = ['10.0.0.2']):
    try:
        cfs = Cassandra_Spatial_Index\
            (cluster_ips = ips,
             keyspace = 'test_spatial_index')
        idx = dummy_index_data()

        for x in idx.iterate():
            cfs.insert(x)

        idx2 = cfs.intersect([(0,0),(0,1),(1,1),(1,0)])

        assert idx.size() == idx2.size()
    finally:
        try:
            cfs.drop_keyspace()
        except:
            pass
