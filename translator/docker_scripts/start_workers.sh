#!/bin/bash

# Check if any arguments are passed
if [[ $# -ge 3 ]]; then
    echo "Usage: $0 <num_of_workers> <workdir>"
else
  if [[ $# -eq 0 ]]; then
    num_workers=2
  elif [[ ! $1 =~ ^-?[0-9]+$ ]]; then
    echo "Usage: $0 <num_of_workers> <workdir>"
    exit 1
  else
    num_workers=$1
  fi

  if [ -n "$2" ]; then
    path=$2
  else
    path=$(pwd)
  fi

  # Get list of available CPU cores (as numbers)
  cpu_cores=($(grep ^processor /proc/cpuinfo | awk '{print $3}'))
  total_cores=${#cpu_cores[@]}

  if (( num_workers > total_cores )); then
    echo "Requested more workers ($num_workers) than available CPU cores ($total_cores)."
    exit 1
  fi

  for ((i = 0; i < num_workers; i++)); do
    port=$((2222 + i))
    # Pick CPU core from the end of the list
    core_index=$((total_cores - 1 - i))
    cpu_core=${cpu_cores[$core_index]}
    
    echo "Starting worker $i on CPU core $cpu_core (port $port)"
    docker run --hostname worker$i \
      --cpuset-cpus="$cpu_core" \
      --name parsl-worker-$i \
      -d -p $port:22 \
      -v $path/data:$path/data \
      -v $path/output:$path/output \
      -v $path/logs:/home/parsl/logs \
      parsl-worker 1
  done
fi

