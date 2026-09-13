# stove0_protocol.WorkflowPlanIntent.materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanintent-materialize:c4325270ce -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-877d2dd0a3"></a>
| Field | Shape |
|---|---|
| <a id="s-d432e85fab"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-5c8bbfa5ce"></a>`distribution` | "stove0-protocol" |
| <a id="s-3e180c16f0"></a>`module` | "stove0_protocol" |
| <a id="s-fb0dbe7feb"></a>`name` | "materialize" |
| <a id="s-bde966a94e"></a>`owner` | "stove0_protocol.WorkflowPlanIntent" |
| <a id="s-1a7441c936"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPlanIntent](stove0-protocol-workflowplanintent.md)

## Governing policies

- <a id="pa-86ba39defa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanIntent.materialize`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32658982903498702fdebedc08cb0af04b593ef4469435d467c253424a7c2e3e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, work: 'WorkIdentity', observations: 'tuple[ObservationEvidence, ...]' = ()) -> 'WorkflowPlan'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "materialize",
  "owner": "stove0_protocol.WorkflowPlanIntent",
  "unit": "member"
}
```
