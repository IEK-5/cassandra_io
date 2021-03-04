#!/bin/env python3

import time

from cassandra_io.files \
    import Cassandra_Files

from cassandra_io.utils \
    import touch_random, remove_file


def write_read(cfs, size = 100):
    touch_random('dummy', size*(1024**2))

    start = time.time()
    cfs.upload('dummy','dummy')
    end = time.time()
    remove_file('dummy')
    write = size / (end-start)

    start = time.time()
    cfs.download('dummy','dummy')
    end = time.time()
    read = size / (end-start)
    remove_file('dummy')
    cfs.delete('dummy')

    return write, read


if __name__ == '__main__':

    try:
        cfs = Cassandra_Files\
            (keyspace_suffix = '_test_files',
             cluster_ips = ['172.17.0.2'],
             replication_args = {'replication_factor': 1})
        w, r = write_read(cfs)
        print("Writing speed: %.2f MB/s" % w)
        print("Reading speed: %.2f MB/s" % r)
    finally:
        try:
            cfs.drop_keyspace()
        except:
            pass
