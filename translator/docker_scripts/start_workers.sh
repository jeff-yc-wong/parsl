#!/bin/bash

# Check if any arguments are passed
if [[ $# -ge 3 ]]; then
    echo "Usage: $0 <num_of_workers> <workdir>"
else
  if [[ $# -eq 0 ]]; then
    num_workers=2
  elif [[ ! $1 =~ ^-?[0-9]+$ ]]; then
    echo "Usage: $0 <num_of_workers> <workdir>"
  else
    num_workers=$1
  fi

  if [ -n "$2" ]; then
    path=$2
  else
    path=$(pwd)
  fi

  for ((i = 0; i < $num_workers; i++)); do
    port=$((2222 + $i))
    docker run --hostname worker$i --cpus=1 --name parsl-worker-$i -d -p $port:22 -v $path/data:$path/data -v $path/output:$path/output -v $path/logs:/home/parsl/logs parsl-worker 1
  done
fi
