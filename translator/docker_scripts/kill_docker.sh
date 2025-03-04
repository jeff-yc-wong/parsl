docker container list -a --filter "name=parsl-worker" -q | xargs -r docker rm -f
