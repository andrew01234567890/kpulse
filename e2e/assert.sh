#!/usr/bin/env bash
# Assert that the kpulse NAR loaded into the broker and its Kafka listener is bound.
# M0-level checks; M1 adds a real Kafka produce/consume round-trip with a pulsar-admin cross-check.
set -euo pipefail

POD=$(kubectl get pod -l app=pulsar-kpulse -o jsonpath='{.items[0].metadata.name}')
echo "Broker pod: ${POD}"

echo "Waiting for kpulse to initialize in the broker log..."
for i in $(seq 1 60); do
  if kubectl logs "${POD}" 2>/dev/null | grep -q "Initialized kpulse"; then
    echo "OK: kpulse protocol handler initialized"
    break
  fi
  if [ "${i}" -eq 60 ]; then
    echo "FAIL: kpulse did not initialize within timeout"
    kubectl logs "${POD}" --tail=200 || true
    exit 1
  fi
  sleep 3
done

echo "Checking the Kafka listener is bound on 9092..."
kubectl exec "${POD}" -- bash -c '
  for i in $(seq 1 30); do
    if (exec 3<>/dev/tcp/127.0.0.1/9092) 2>/dev/null; then
      echo "OK: Kafka listener accepting connections on 9092"
      exit 0
    fi
    sleep 2
  done
  echo "FAIL: Kafka listener not open on 9092"
  exit 1
'
echo "e2e assertions passed."
