#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [NEW_OUTPUT_DIRECTORY]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  out="$1"
else
  out="$repo_root/var/diagnose-waiting-$(date -u +%Y%m%dT%H%M%S)"
fi
case "$out" in
  /*) abs_out="$out" ;;
  *) abs_out="$repo_root/$out" ;;
esac
if [[ -e "$abs_out" ]]; then
  echo "refusing to reuse existing output directory: $abs_out" >&2
  exit 2
fi
mkdir -p "$abs_out"

export WAVEGATE_DIAG_OUT="$abs_out"
go test -count=1 -run '^TestDiagnoseWaitingCauses$' ./server -v 2>&1 | tee "$abs_out/go-test.log"

go build -trimpath -o "$abs_out/wavegate" ./cmd/wavegate
"$abs_out/wavegate" inspect-state \
  -state-directory "$abs_out/mismatch-blocked/state" | tee "$abs_out/inspect-blocked-state.json"
"$abs_out/wavegate" inspect-state \
  -state-directory "$abs_out/mismatch-successor/state" | tee "$abs_out/inspect-successor-state.json"
"$abs_out/wavegate" verify-receipt \
  -input "$abs_out/old-owner.receipt.v1.json" | tee "$abs_out/verify-receipt.txt"
sha256sum "$abs_out/old-owner.receipt.v1.json" | tee "$abs_out/old-owner.receipt.sha256"

jq '{
  budgetReason: .budget.condition.reason,
  endpointReason: .endpointNotReady.condition.reason,
  mismatchReasonAfterRestart: .stableRevisionMismatch.restart.conditionAfterReconciles.reason,
  blockedQuota: .stableRevisionMismatch.quotaWhileBlocked,
  owner: .stableRevisionMismatch.owner,
  lastCommittedBinding: {
    resourceVersion: .stableRevisionMismatch.lastActuallyCommittedBinding.resourceVersion,
    operation: .stableRevisionMismatch.lastActuallyCommittedBinding.operation,
    ownerUid: .stableRevisionMismatch.lastActuallyCommittedBinding.ownerUid,
    stable: .stableRevisionMismatch.lastActuallyCommittedBinding.rule.stable,
    candidateWeight: .stableRevisionMismatch.lastActuallyCommittedBinding.rule.candidateWeight
  },
  restart: .stableRevisionMismatch.restart.inspectRecovery,
  correctSuccessor: {
    name: .stableRevisionMismatch.correctSuccessorDeclaration.name,
    uid: .stableRevisionMismatch.correctSuccessorDeclaration.uid,
    stable: .stableRevisionMismatch.correctSuccessorDeclaration.stable,
    bindingResourceVersion: .stableRevisionMismatch.correctSuccessorDeclaration.newBinding.resourceVersion,
    ownerUid: .stableRevisionMismatch.correctSuccessorDeclaration.newBinding.ownerUid,
    candidateWeight: .stableRevisionMismatch.correctSuccessorDeclaration.newBinding.rule.candidateWeight
  },
  oldReceiptUnchanged: .stableRevisionMismatch.oldReceipt.deepUnchangedAfterSuccessor
}' "$abs_out/evidence.v1.json"
