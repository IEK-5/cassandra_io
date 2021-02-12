#!/bin/bash

cd $(git rev-parse --show-toplevel)


function set_defaults {
    image_path="loop_storage_image"
    mount_path="loop_storage"
    size="1G"
}


function print_help {
    echo "Usage $0 [OPTIONS]"
    echo
    echo "Create a loop storage"
    echo
    echo "  -h,--help         print this page"
    echo
    echo "  --image_path      path to the image"
    echo "                    Default: ${image_path}"
    echo
    echo "  --mount_path      path for the mount directory"
    echo "                    Default: ${mount_path}"
    echo
    echo "  --size            size of the storage"
    echo "                    Default: ${size}"
    echo
}


function parse_args {
    for i in "$@"
    do
        case "${i}" in
            --image_path=*)
                image_path="${i#*=}"
                shift
                ;;
            --mount_path=*)
                mount_path="${i#*=}"
                shift
                ;;
            --size=*)
                size="${i#*=}"
                shift
                ;;
            -h|--help)
                print_help
                exit
        esac
    done
}


function create_mount_storage {
    if [ ! -f "${image_path}" ]
    then
        # truncate takes less initial space, but slows down on allocation
        # fallocate -l "${size}" "${storage_path}"
        truncate -s "${size}" "${image_path}"
        mkfs.ext4 "${image_path}"
        mkdir -p "${mount_path}"
    fi

    sudo mount -o loop "${image_path}" "${mount_path}"
}


set_defaults
parse_args $@
create_mount_storage
