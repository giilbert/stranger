#!/bin/bash

C_RESET=$(tput sgr0)

C_RED=$(tput setaf 1)
C_BLUE=$(tput setaf 4)
C_PURPLE=$(tput setaf 5)

echo ">>> ${C_PURPLE}stranger${C_RESET} - server entrypoint script"
echo " |- $(docker --version)"

echo ""

# Start docker service in background
echo "> ${C_PURPLE}Booting${C_RESET} - Spawned dockerd"

rm -rf /var/run/docker.pid

dockerd-entrypoint.sh &

time=0
# Wait until the docker service is up
while ! docker ps >/dev/null 2>&1; do
  echo "> ${C_PURPLE}Booting${C_RESET} - Waiting on dockerd to be ready ($time)"
  time=$((time + 1))
  if [ $time -gt 10 ]; then
    echo "> ${C_RED}Error${C_RESET} - Timeout waiting on dockerd to be ready"
    exit 1
  fi
  sleep 1
done

echo "> ${C_PURPLE}Booting${C_RESET} - dockerd ready -> importing images \`ubuntu\`"

docker pull ubuntu:latest

# # Import pre-installed images
# for file in /images/*.tar; do
#   echo "> ${C_PURPLE}Booting${C_RESET} - Importing image $file"
#   docker load -q <$file
# done

/usr/local/bin/stranger_api_server