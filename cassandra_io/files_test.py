from cassandra_io.files import \
    Cassandra_Files

from cassandra_io.utils import \
    touch_random, get_hash, file_hash, \
    remove_file


def test_files(ips = ['172.17.0.2']):
    try:
        touch_random('dummy', 5242880)
        cfs = Cassandra_Files(keyspace_suffix = '_test_files',
                              cluster_ips = ips)

        cfs.upload('dummy','dummy')
        cfs.upload('dummy','dummy')

        cfs.cleanup()

        assert 'dummy' in cfs
        assert 'dummy_' not in cfs

        cfs.download('dummy','dummy_test')
        h_original = file_hash('dummy')
        h_download = file_hash('dummy_test')
        h_bytesio = get_hash\
            (cfs.download_bytesio('dummy').read())

        assert h_original == h_download
        assert h_original == h_bytesio

        cfs.delete('dummy')
        assert 'dummy' not in cfs
    finally:
        try:
            cfs.drop_keyspace()
        except:
            pass
        remove_file('dummy')
        remove_file('dummy_test')
