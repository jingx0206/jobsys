#!/usr/bin/env bash
# Copies the locally built jobsys images onto every Kubernetes node.
#
# Docker Desktop's kind-based cluster runs its own containerd, which cannot see
# images in Docker's image store, so pods fail with ErrImageNeverPull. This
# does what `kind load docker-image` does: stream `docker save` into
# `ctr images import` on each node, through a short-lived privileged pod that
# enters the node's mount namespace. Run it after every image rebuild, then
# `kubectl -n jobsys rollout restart deployment` to pick the new images up.
set -euo pipefail

images=(jobsys-api:dev jobsys-worker:dev)
ns=kube-system

for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  pod="image-loader-${node}"
  kubectl -n "$ns" delete pod "$pod" --ignore-not-found --wait=true >/dev/null
  kubectl -n "$ns" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $pod
spec:
  nodeName: $node
  hostPID: true
  restartPolicy: Never
  containers:
    - name: loader
      image: alpine:3.22
      command: ["sleep", "600"]
      securityContext:
        privileged: true
EOF
  kubectl -n "$ns" wait --for=condition=Ready "pod/$pod" --timeout=120s >/dev/null

  echo "loading ${images[*]} onto $node"
  docker save "${images[@]}" |
    kubectl -n "$ns" exec -i "$pod" -- nsenter -t 1 -m -- ctr -n k8s.io images import -

  kubectl -n "$ns" delete pod "$pod" --wait=false >/dev/null
done
