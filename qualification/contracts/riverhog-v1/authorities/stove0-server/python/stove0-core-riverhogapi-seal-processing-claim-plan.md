# stove0_core.RiverhogApi.seal_processing_claim_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-seal-processing-claim-plan:5a270d514e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-570d0f39b9"></a>
| Field | Shape |
|---|---|
| <a id="s-7f3bc291bc"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-123d1ba726"></a>`distribution` | "stove0-server" |
| <a id="s-407e89e3ff"></a>`module` | "stove0_core" |
| <a id="s-6169869ba8"></a>`name` | "seal_processing_claim_plan" |
| <a id="s-9aa249f4cc"></a>`owner` | "stove0_core.RiverhogApi" |
| <a id="s-8be09261e9"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-2123d2abeb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.seal_processing_claim_plan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 886094a024749fed07eea945403e6a80a08fb93ba53de908fa3609bb4bedbb76 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', execution_id: 'str', controller_evidence: 'Mapping[str, Any]', controller_evidence_sha256: 'str', operation_id: 'str', operation_sha256: 'str', input_artifacts: 'Iterable[Mapping[str, Any]]', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_processing_claim_plan",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```
