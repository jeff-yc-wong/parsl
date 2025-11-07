#!/bin/bash

sudo mkdir -p /run/sshd
sudo /usr/sbin/sshd &

if [[ $# -eq 2 && $2 =~ ^-?[0-9]+$  ]]; then
  sudo usermod -u $2 parsl
fi

if [[ $# -ge 1 && $1 =~ ^-?[0-9]+$  ]]; then
  echo "CPU_SPEED=$1" | sudo tee -a /etc/environment
# else
#   echo "CPU_SPEED=1" | sudo tee -a /etc/environment
fi

tail -f /dev/null

