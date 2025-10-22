#!/bin/bash

C_RESET=$(tput sgr0)
C_PURPLE=$(tput setaf 5)

print_title() {
    local title=$1
    local message=$2
    echo "${C_PURPLE}${title}${C_RESET}: ${message}"
}

print_title "stranger api server" "starting server entrypoint script"
echo " - $(docker --version)"
echo ""

# start docker service in background
print_title "docker" "setting up docker"

rm -rf /var/run/docker.pid

# on Fly, unmount /sys/fs/cgroup/cpu,cpuacct if it is mounted and then remount
# it as only cpu controller
if ! mountpoint -q /sys/fs/cgroup/cpu; then
    print_title "fly" "applying cgroups fix"
    if mountpoint -q /sys/fs/cgroup/cpu,cpuacct; then
        print_title "fly" "unmounting /sys/fs/cgroup/cpu,cpuacct"
        umount /sys/fs/cgroup/cpu,cpuacct
    fi

    print_title "fly" "remounting /sys/fs/cgroup/cpu"
    mkdir -p /sys/fs/cgroup/cpu
    chmod 0555 /sys/fs/cgroup/cpu
    mount -t cgroup -o cpu cpu /sys/fs/cgroup/cpu
fi

dockerd-entrypoint.sh &

/usr/local/bin/stranger_api_server