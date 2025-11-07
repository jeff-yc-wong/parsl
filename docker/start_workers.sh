#!/bin/bash

# ======================
# Parsl Worker Launcher
# ======================
# Usage:
#   ./start_workers.sh <num_of_workers> [-s <start_index>] [-d <workdir>] [-h]
#
# Example:
#   ./start_workers.sh 4 -s 10 -d /data/project
#
# Description:
#   Launches a set of Parsl worker containers pinned to individual CPU cores.
# ======================

usage() {
    echo "Usage: $0 <num_of_workers> [-s <start_index>] [-d <workdir>] [-cs <core_speed>] [-h]"
    echo
    echo "Options:"
    echo "  <num_of_workers>   Number of workers to launch (required)"
    echo "  -s, --start        Starting index for worker names (default: 0)"
    echo "  -d, --dir          Path to working directory (default: current dir)"
    echo "  -cs, --core-speed  Core speed of workers as a float value"
    echo "  -h, --help         Show this help message and exit"
    echo
    echo "Example:"
    echo "  $0 3 -s 5 -d /mnt/project -cs 2.5"
    exit 0
}

# --- Argument parsing ---
if [[ $# -lt 1 ]]; then
    usage
fi

# First positional argument = num_of_workers
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
elif [[ ! $1 =~ ^[0-9]+$ ]]; then
    echo "Error: <num_of_workers> must be a positive integer."
    usage
fi
num_workers=$1
shift

# Defaults
start_index=0
path=$(pwd)

# --- Parse flags ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--start)
            shift
            if [[ ! $1 =~ ^[0-9]+$ ]]; then
                echo "Error: -s requires a non-negative integer."
                usage
            fi
            start_index=$1
            ;;
        -d|--dir)
            shift
            if [[ -z "$1" ]]; then
                echo "Error: -d requires a directory path."
                usage
            fi
            path=$1
            ;;
        -cs|--core-speed)
            shift
            if [[ ! $1 =~ ^[0-9]*\.?[0-9]+$ ]]; then
                echo "Error: -cs requires a positive number (integer or float)."
                usage
            fi
            core_speed=$1
            ;;
        -h|--help)
            usage
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            ;;
        *)
            echo "Unexpected argument: $1"
            usage
            ;;
    esac
    shift
done

# --- CPU core detection ---
cpu_cores=($(grep ^processor /proc/cpuinfo | awk '{print $3}'))
total_cores=${#cpu_cores[@]}

if (( num_workers > total_cores )); then
    echo "Requested more workers ($num_workers) than available CPU cores ($total_cores)."
    exit 1
fi

# --- Launch workers ---
for ((i = 0; i < num_workers; i++)); do
    port=$((2222 + i))
    core_index=$((total_cores - 1 - i))
    cpu_core=${cpu_cores[$core_index]}

    worker_index=$((start_index + i))
    echo "Starting worker$worker_index on CPU core $cpu_core (port $port)"

    docker run --hostname worker$worker_index \
        --cpuset-cpus="$cpu_core" \
        --name parsl-worker-$worker_index \
        -d -p $port:22 \
        -v "$path:/home/parsl/sus_env" \
        parsl-worker $core_speed
done
