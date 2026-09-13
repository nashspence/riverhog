# stove0_core.RiverhogControlPort.seal_execution

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-seal-execution:0d7af9b6ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-71c456b30d"></a>
| Field | Shape |
|---|---|
| <a id="s-0fbe70d8ea"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-24212369c7"></a>`distribution` | "stove0-server" |
| <a id="s-3c231ca196"></a>`module` | "stove0_core" |
| <a id="s-fcc3b6b795"></a>`name` | "seal_execution" |
| <a id="s-73669aafdd"></a>`owner` | "stove0_core.RiverhogControlPort" |
| <a id="s-4fbc757e5e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-6b85cc91eb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.seal_execution`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60b4c6db0230d615fb7e3677f386c3ca3a8d6fe2a47e4b9e2b0152751668c7b5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', plan: 'WorkflowPlan', target_plan: 'TargetPlan', inputs: 'Iterable[ArtifactSubject]') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_execution",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```
