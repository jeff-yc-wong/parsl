if [[ $1 -eq 2 ]]; then
	docker build --target builder -t parsl-worker --no-cache --build-arg USER_UID=$(id -u) .
elif [[ $1 -eq 1 ]]; then
	docker build --target builder -t parsl-worker --build-arg CACHEBUST=$(date +%s) --build-arg USER_UID=$(id -u) .
else
	docker build --target builder -t parsl-worker --build-arg USER_UID=$(id -u) .
fi
