#!/usr/bin/env bash
# Run the KIND e2e flow locally, mirroring .github/workflows/kind-e2e.yml.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="${CLUSTER:-kpulse-e2e-local}"

./gradlew :kpulse-protocol-handler:nar --console=plain
cp kpulse-protocol-handler/build/libs/kpulse-protocol-handler-*.nar e2e/kpulse.nar
docker build -t kpulse-broker:e2e e2e

kind create cluster --name "${CLUSTER}" 2>/dev/null || echo "cluster ${CLUSTER} already exists"
kind load docker-image kpulse-broker:e2e --name "${CLUSTER}"

kubectl apply -f e2e/pulsar-standalone.yaml
kubectl rollout status deployment/pulsar-kpulse --timeout=300s
bash e2e/assert.sh

echo
echo "Local e2e passed. Tear down with: kind delete cluster --name ${CLUSTER}"
