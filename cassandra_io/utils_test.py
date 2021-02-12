import geohash
from shapely import geometry
import numpy as np

from cassandra_io.utils import \
    touch_random, \
    get_hash, file_hash, \
    remove_file, \
    read_by_chunks, write_by_chunks, write_bytesio_by_chunk, \
    bbox2hash


def test_read_write_chunks():
    try:
        touch_random('dummy')
        h_original = file_hash('dummy')

        write_by_chunks\
            (read_by_chunks\
             ('dummy', chunk = 1048576),
             'dummy_test')
        h_written = file_hash('dummy_test')

        h_bytes = get_hash\
            (write_bytesio_by_chunk\
             (read_by_chunks\
              ('dummy', chunk = 1048576)).read())

        assert h_original == h_written
        assert h_original == h_bytes

        assert h_original == get_hash\
            (write_bytesio_by_chunk\
             (read_by_chunks\
              ('dummy', chunk = 113)).read())
    finally:
        remove_file('dummy')
        remove_file('dummy_test')


def bbox2hash_one(bbox, hash_length):
    # just verify that the resulting hash boxes cover the whole bbox
    hashes = bbox2hash(bbox = bbox, hash_length = hash_length)

    arr = np.array\
        ([[x['s'],x['w'],x['n'],x['e']] \
          for x in [geohash.bbox(x) \
                    for x in bbox2hash(bbox,hash_length)]])
    assert min(arr[:,0]) <= bbox[0]
    assert min(arr[:,1]) <= bbox[1]
    assert max(arr[:,2]) >= bbox[2]
    assert max(arr[:,3]) >= bbox[3]


def test_bbox2hash():
    # large areas
    bbox2hash_one([20,5,60,10],1)
    bbox2hash_one([20,5,60,10],2)
    bbox2hash_one([20,5,60,10],3)
    bbox2hash_one([20,5,60,10],4)

    # area next to borders
    bbox2hash_one([20,0,60,10],4)
    bbox2hash_one([0,0,5,5],4)
    bbox2hash_one([88,-10,89,10],4)
    bbox2hash_one([-89,-10,-88,10],4)
    bbox2hash_one([-89,-179,-88,-175],4)

    # very large areas
    bbox2hash_one([-89,-179,89,179],3)
    # bbox2hash_one([-89,-179,89,179],2) # this seems to be geohash bug

    # smaller areas, more precise hash
    bbox2hash_one([0,0,.1,.1],5)
    bbox2hash_one([0,0,.1,.1],6)
    bbox2hash_one([0,0,.1,.1],7)

    # random areas
    for i in range(100):
        a = np.random.random()*175-85
        b = np.random.random()*355-175
        bbox2hash_one([a,b,a+0.01,b+0.01],7)
        bbox2hash_one([a,b,a+0.01,b+0.01],6)
        bbox2hash_one([a,b,a+0.01,b+0.01],5)
        bbox2hash_one([a,b,a+0.1,b+0.1],4)
        bbox2hash_one([a,b,a+0.1,b+0.1],3)
