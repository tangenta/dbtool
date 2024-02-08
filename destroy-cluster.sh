#!/bin/bash
# kill while true
ps fjx | grep "[k]ubectl" | tr -s '\t' ' ' | cut -d ' ' -f 2 | xargs -I_ kill _
# kill kubectl port-forward
pgrep -lfa kubectl | cut -d ' ' -f1 | xargs -I_ kill _
kubectl delete tc tc -n tidb-cluster
kubectl delete pvc -n tidb-cluster -l app.kubernetes.io/instance=tc,app.kubernetes.io/managed-by=tidb-operator
kubectl get pv -l app.kubernetes.io/namespace=tidb-cluster,app.kubernetes.io/managed-by=tidb-operator,app.kubernetes.io/instance=tc -o name | xargs -I {} kubectl patch {} -p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
kubectl delete namespace tidb-cluster
