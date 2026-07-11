#!/usr/bin/env bash
# Run the KIND e2e flow locally, mirroring the kind-e2e job in .github/workflows/ci.yml.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="${CLUSTER:-kpulse-e2e-local}"
OXIA_IMAGE_SOURCE="oxia/oxia@sha256:ae01c97d1e3e686e66656ad4ef71cf9ec2cb834eec250e5598eb729bc30e43fc" # v0.16.7 linux/amd64
OXIA_IMAGE="oxia/oxia:0.16.7-kpulse"

./gradlew :kpulse-protocol-handler:nar :e2e-client:installDist --console=plain
cp kpulse-protocol-handler/build/libs/kpulse-protocol-handler-*.nar e2e/kpulse.nar
rm -rf e2e/e2e-client
cp -r e2e-client/build/install/e2e-client e2e/e2e-client
docker build --provenance=false -t kpulse-broker:e2e e2e
docker pull "${OXIA_IMAGE_SOURCE}"
docker tag "${OXIA_IMAGE_SOURCE}" "${OXIA_IMAGE}"

kind create cluster --name "${CLUSTER}" 2>/dev/null || echo "cluster ${CLUSTER} already exists"
kubectl config use-context "kind-${CLUSTER}" >/dev/null
kind load docker-image kpulse-broker:e2e --name "${CLUSTER}"
kind load docker-image "${OXIA_IMAGE}" --name "${CLUSTER}"

# Both services use pod-local storage. Reset them as a unit so rerunning after a manifest or image
# change cannot pair stale Oxia metadata with a fresh BookKeeper directory.
kubectl delete -f e2e/pulsar-standalone.yaml --ignore-not-found --wait=true
kubectl apply -f e2e/pulsar-standalone.yaml
kubectl rollout status deployment/oxia --timeout=120s
kubectl rollout status deployment/pulsar-kpulse --timeout=300s
bash e2e/assert.sh

echo
echo "Local e2e passed. Tear down with: kind delete cluster --name ${CLUSTER}"
