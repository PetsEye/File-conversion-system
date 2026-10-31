#!/usr/bin/env bash
# usage: sudo deploy/install.sh coordinator
#        sudo deploy/install.sh worker HOST:PORT [extra worker flags...]
set -euo pipefail

role=${1:-}
cd "$(dirname "$0")/.."

if [[ $role != coordinator && $role != worker ]]; then
    echo "usage: $0 coordinator | worker HOST:PORT [flags...]" >&2
    exit 2
fi

cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
install -m 755 build/mrnode /usr/local/bin/mrnode
install -m 644 "deploy/mreduce-$role.service" /etc/systemd/system/

if [[ $role == worker ]]; then
    coord=${2:?worker needs the coordinator HOST:PORT}
    shift 2
    printf 'MREDUCE_COORDINATOR=%s\nMREDUCE_WORKER_ARGS=--name %s %s\n' \
        "$coord" "$(hostname)" "$*" > /etc/default/mreduce-worker
fi

systemctl daemon-reload
systemctl enable --now "mreduce-$role"
systemctl restart "mreduce-$role"
systemctl --no-pager status "mreduce-$role" | head -5
