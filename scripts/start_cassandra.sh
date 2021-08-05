#!/bin/bash

cd $(dirname $(realpath "$0"))
cd $(git rev-parse --show-toplevel)


function set_defaults {
    mount_point="$(realpath ./storage_mount_point)"
    heap_newsize="100M"
    max_heap_size="500M"
    docker_name="cassandra_storage"
    docker_maxmemory="3g"
    broadcast_address=""
    seed_address=""
    cassandra_tag="latest"
}


function print_help {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Join to/Start a cassandra storage"
    echo
    echo "  -h,--help         print this page"
    echo
    echo "  --mnt             set a mount point of the storage."
    echo "                    Directory is created if it does not exist."
    echo "                    Default: ${mount_point}"
    echo
    echo "  --heap_newsize, --max_heap_size"
    echo "                    set HEAP_NEWSIZE and MAX_HEAP_SIZE for the cassandra storage."
    echo "                    Default: heap_newsize=${heap_newsize}"
    echo "                             max_heap_size=${max_heap_size}"
    echo
    echo "  --docker_name     Name of the docker container to run."
    echo "                    Default: ${docker_name}"
    echo
    echo "  --cassandra_tag   tag of the cassandra build"
    echo "                    Default: ${cassandra_tag}"
    echo
    echo "  --broadcast       Broadcast address"
    echo "                    Local machine ip of the preferred network."
    echo "                    Leave empty to use default docker overlay."
    echo "                    Default: ${broadcast_address}"
    echo
    echo "  --seed            Seed address"
    echo "                    Remote machine with running cassandra storage"
    echo "                    and the exposed 7000 port."
    echo "                    Leave empty to run a single node only."
    echo "                    Default: ${seed_address}"
    echo
    echo "  --maxmemory       Hard limit on the memory usage"
    echo "                    Default: ${docker_maxmemory}"
    echo
}


function parse_args {
    for i in "$@"
    do
        case "${i}" in
            --mnt=*)
                mount_point="${i#*=}"
                shift
                ;;
            --heap_newsize=*)
                heap_newsize="${i#*=}"
                shift
                ;;
            --max_heap_size=*)
                max_heap_size="${i#*=}"
                shift
                ;;
            --docker_name=*)
                docker_name="${i#*=}"
                shift
                ;;
            --broadcast=*)
                broadcast_address="${i#*=}"
                shift
                ;;
            --seed=*)
                seed_address="${i#*=}"
                shift
                ;;
            --cassandra_tag=*)
                cassandra_tag="${i#*=}"
                shift
                ;;
	        --maxmemory=*)
		        docker_maxmemory="${i#*=}"
		        shift
		        ;;
            -h|--help)
                print_help
                exit
        esac
    done
}


function start_docker {
    [ ! -z "${broadcast_address}" ] && \
        broadcast_address="-e CASSANDRA_BROADCAST_ADDRESS=${broadcast_address} \
                           -p ${broadcast_address}:7000:7000 \
                           -p ${broadcast_address}:7001:7001 \
                           -p ${broadcast_address}:7199:7199 \
                           -p ${broadcast_address}:9042:9042 \
                           -p ${broadcast_address}:9160:9160"
    [ ! -z "${seed_address}" ] && \
        seed_address="-e CASSANDRA_SEEDS=${seed_address}"

    docker container prune
    docker run \
	       --restart always \
	       --memory "${docker_maxmemory}" \
           --name "${docker_name}" \
           -v "${mount_point}":/var/lib/cassandra \
           -v "$(realpath ./cassandra.yaml)":/etc/cassandra/cassandra.yaml \
           -e HEAP_NEWSIZE="${heap_newsize}" \
           -e MAX_HEAP_SIZE="${max_heap_size}" \
           ${broadcast_address} ${seed_address} \
           -d cassandra:${cassandra_tag}
}


set_defaults
parse_args $@
[ ! -e "${mount_point}" ] && mkdir -p "${mount_point}"
start_docker
