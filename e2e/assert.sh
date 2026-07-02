#!/usr/bin/env bash
# Assert that the kpulse NAR loaded into the broker and its Kafka listener is bound.
# M0-level checks; M1 adds a real Kafka produce/consume round-trip with a pulsar-admin cross-check.
#
# Logs are captured to a file before grepping: piping `kubectl logs` straight into `grep -q`
# lets grep exit on first match while kubectl is still streaming, which raises SIGPIPE and — under
# `set -o pipefail` — is reported as a pipeline failure even though the match succeeded.
set -euo pipefail

POD=$(kubectl get pod -l app=pulsar-kpulse -o jsonpath='{.items[0].metadata.name}')
echo "Broker pod: ${POD}"

log_file=$(mktemp)
trap 'rm -f "${log_file}"' EXIT

# Re-fetch the log every iteration and require BOTH init and bind: the bind line ("Starting kpulse
# Kafka protocol handler" / "Successfully bind protocol ... protocol=kafka") is logged in start(),
# strictly after "Initialized kpulse" in initialize(), so a single early snapshot could miss it.
echo "Waiting for kpulse to initialize and bind its Kafka listener..."
ready=false
for _ in $(seq 1 60); do
  kubectl logs "${POD}" >"${log_file}" 2>/dev/null || true
  if grep -q "Initialized kpulse" "${log_file}" \
    && grep -qE "bind protocol .*protocol=kafka|Starting kpulse Kafka protocol handler" "${log_file}"; then
    ready=true
    break
  fi
  sleep 3
done
if [ "${ready}" != true ]; then
  echo "FAIL: kpulse did not initialize and bind the Kafka listener within timeout"
  kubectl logs "${POD}" --tail=200 || true
  exit 1
fi
echo "OK: kpulse initialized and bound the Kafka protocol listener"

echo "Checking the Kafka listener accepts connections on 9092..."
port_open=false
for _ in $(seq 1 30); do
  if kubectl exec "${POD}" -- bash -c 'exec 3<>/dev/tcp/127.0.0.1/9092' 2>/dev/null; then
    port_open=true
    break
  fi
  sleep 2
done
if [ "${port_open}" = true ]; then
  echo "OK: Kafka listener accepting connections on 9092"
else
  echo "FAIL: Kafka listener not open on 9092"
  exit 1
fi

echo "e2e assertions passed."
