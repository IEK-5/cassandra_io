#!/bin/bash

cd $(git rev-parse --show-toplevel)

storage_path="./storage_image"
size="1G"

if [ ! -f "${storage_path}" ]
then
    # truncate takes less initial space, but slows down on allocation
    # fallocate -l "${size}" "${storage_path}"
    truncate -s "${size}" "${storage_path}"
    mkfs.ext4 "${storage_path}"
    mkdir -p "${storage_path}_mnt"
fi

sudo mount -o loop "${storage_path}" "${storage_path}_mnt"

docker run \
       --name some-cassandra \
       -v $(pwd)/"${storage_path}_mnt":/var/lib/cassandra \
       -e HEAP_NEWSIZE=1M -e MAX_HEAP_SIZE=100M \
       -d cassandra

# docker run \
#        --name some-cassandra2 \
#        -v $(pwd)/cassandra_2:/var/lib/cassandra \
#        -e CASSANDRA_SEEDS=172.17.0.2 \
#        -e HEAP_NEWSIZE=1M -e MAX_HEAP_SIZE=100M \
#        -d cassandra

