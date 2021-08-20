import os
import geohash
import hashlib
import itertools

import numpy as np

from io import BytesIO


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
    try:
        if '' != os.path.dirname(ofn):
            os.makedirs(os.path.dirname(ofn), exist_ok = True)
        with open(ofn, 'wb') as f:
            for data in generator:
                f.write(data)
    except Exception as e:
        remove_file(ofn)
        raise e


def write_bytesio_by_chunk(generator):
    """Write output file chunk in memory

    :generator: generator that yield byte chunks

    :return: BytesIO buffer
    """
    res = BytesIO()

    for data in generator:
        res.write(data)

    res.seek(0)
    return res


def hash_any(key):
    h = hashlib.sha512()

    if isinstance(key, bytes):
        h.update(key)
        return h.hexdigest()

    if not isinstance(key, (tuple, list)):
        h.update(str(key).encode('utf-8'))
        return h.hexdigest()

    for x in key:
        h.update(hash_any(x).encode('utf-8'))
    return h.hexdigest()


def touch(fname, size = 10485760):
    with open(fname, 'wb') as f:
        f.seek(size - 1)
        f.write(b'\0')


def touch_random(fname, size = 10485760):
    with open(fname, 'wb') as f:
        f.write(os.urandom(size))


def get_hash(data):
    h = hashlib.sha512()
    h.update(data)
    return h.hexdigest()


def file_hash(fn):
    with open(fn,'rb') as f:
        return get_hash(f.read())


def remove_file(fn):
    try:
        if fn:
            os.remove(fn)
    except:
        pass


def bbox2hash(bbox, hash_length):
    """Split bounding box coordinates onto smaller boxes

    :bbox: (lat_min, lon_min, lat_max, lon_max)

    :hash_length: maximum of the geohash string

    :return: list of hashes meshing the bbox
    """
    by = geohash.decode_exactly\
        (geohash.encode\
         (*bbox[:2])[:hash_length])[2:]
    res = (geohash.encode(*x)[:hash_length] \
           for x in itertools.product\
           (np.arange(bbox[0],bbox[2] + by[0],by[0]),
            np.arange(bbox[1],bbox[3] + by[1],by[1])))
    return list(set(res))


def bboxes2hash(bbox_list, hash_length):
    """Same as bbox2hash, but accept an iterable

    :bbox_list: list of bounding boxes

    """
    res = set()
    for bbox in bbox_list:
        res = res.union\
            (bbox2hash\
             (bbox = bbox,
              hash_length = hash_length))

    return list(res)
