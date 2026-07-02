#!/usr/bin/env bash
# M0 preflight: the kpulse NAR loaded into the broker and its Kafka listener is bound.
# M1 exit criterion: a real Kafka client produces and consumes through the deployed broker,
# cross-checked via pulsar-admin.
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

CLIENT=/pulsar/e2e-client/bin/e2e-client
BOOTSTRAP=127.0.0.1:9092
TOPIC="kpulse-e2e-$(date +%s)-$$"
RECORD_COUNT=5

# Verify the produced/consumed stdout lines are "<TAG>\t<offset>\t<key>\t<value>" for offsets 0..N-1
# with keys/values "k<i>"/"v<i>", in order — proof the round-trip preserved both data and ordering.
verify_records() {
  local tag="$1" output="$2" expected=0
  while IFS=$'\t' read -r line_tag offset key value; do
    [ "${line_tag}" = "${tag}" ] || continue
    if [ "${offset}" != "${expected}" ] || [ "${key}" != "k${expected}" ] || [ "${value}" != "v${expected}" ]; then
      echo "FAIL: unexpected ${tag} record at index ${expected}: ${line_tag} ${offset} ${key} ${value}"
      return 1
    fi
    expected=$((expected + 1))
  done <<<"${output}"
  if [ "${expected}" -ne "${RECORD_COUNT}" ]; then
    echo "FAIL: expected ${RECORD_COUNT} ${tag} records, got ${expected}"
    return 1
  fi
}

echo
echo "Producing ${RECORD_COUNT} records to ${TOPIC} via a real Kafka client (kubectl exec)..."
if ! produce_out=$(kubectl exec "${POD}" -- "${CLIENT}" produce "${BOOTSTRAP}" "${TOPIC}" "${RECORD_COUNT}"); then
  echo "FAIL: kubectl exec produce failed"
  echo "${produce_out}"
  exit 1
fi
echo "${produce_out}"
if ! verify_records PRODUCED "${produce_out}"; then
  exit 1
fi
echo "OK: produced ${RECORD_COUNT} records with sequential offsets 0..$((RECORD_COUNT - 1))"

echo
echo "Consuming ${RECORD_COUNT} records back from ${TOPIC} (partition 0, from earliest, manual assign)..."
if ! consume_out=$(kubectl exec "${POD}" -- "${CLIENT}" consume "${BOOTSTRAP}" "${TOPIC}" "${RECORD_COUNT}" 60000); then
  echo "FAIL: kubectl exec consume failed"
  echo "${consume_out}"
  exit 1
fi
echo "${consume_out}"
if ! verify_records CONSUMED "${consume_out}"; then
  exit 1
fi
echo "OK: consumed ${RECORD_COUNT} records with matching keys/values at offsets 0..$((RECORD_COUNT - 1))"

echo
echo "Cross-checking via pulsar-admin that the records landed in Pulsar's managed ledger..."
if ! topics=$(kubectl exec "${POD}" -- bin/pulsar-admin topics list public/default); then
  echo "FAIL: pulsar-admin topics list failed"
  exit 1
fi
if ! printf '%s' "${topics}" | grep -qF "persistent://public/default/${TOPIC}"; then
  echo "FAIL: pulsar-admin topics list does not include persistent://public/default/${TOPIC}"
  echo "${topics}"
  exit 1
fi
# Kafka produce/fetch bypass Pulsar's per-topic msgInCounter/msgOutCounter (those are wired to the
# Pulsar producer/consumer publish path, not kpulse's), so the managed-ledger entry count from
# stats-internal is the reliable cross-check that the bytes actually persisted — mirrors
# KafkaVerticalSliceIntegrationTest's ManagedLedger#getNumberOfEntries() assertion.
if ! stats_internal=$(kubectl exec "${POD}" -- bin/pulsar-admin topics stats-internal "persistent://public/default/${TOPIC}"); then
  echo "FAIL: pulsar-admin topics stats-internal failed"
  exit 1
fi
entries_added=$(printf '%s' "${stats_internal}" | grep -oE '"entriesAddedCounter"[[:space:]]*:[[:space:]]*[0-9]+' | grep -oE '[0-9]+$' || true)
if [ -z "${entries_added}" ] || [ "${entries_added}" -lt "${RECORD_COUNT}" ]; then
  echo "FAIL: pulsar-admin reports entriesAddedCounter=${entries_added:-<none>}, expected >= ${RECORD_COUNT}"
  echo "${stats_internal}"
  exit 1
fi
echo "OK: pulsar-admin confirms entriesAddedCounter=${entries_added} (>= ${RECORD_COUNT}) for persistent://public/default/${TOPIC}"

echo "e2e assertions passed."
