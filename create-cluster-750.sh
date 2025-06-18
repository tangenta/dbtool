#!/bin/bash
set -euo pipefail
kubectl create namespace tidb-cluster
kubectl config set-context --current --namespace="tidb-cluster"
kubectl -n tidb-cluster apply -f resource/tidb-cluster-v750.yaml
kubectl -n tidb-cluster wait --for=condition=Ready --timeout=300s tc/tc
set +euo pipefail
while true; do kubectl -n tidb-cluster port-forward tc-tidb-0 4000:4000 2>&1 >/dev/null; done &
while true; do kubectl -n tidb-cluster port-forward tc-tidb-0 10080:10080 2>&1 >/dev/null; done &
