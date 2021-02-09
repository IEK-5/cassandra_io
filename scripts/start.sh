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

docker container prune
docker run \
       --name some-cassandra \
       -v $(pwd)/"${storage_path}_mnt":/var/lib/cassandra \
       -e HEAP_NEWSIZE=1M -e MAX_HEAP_SIZE=100M \
       -d cassandra
