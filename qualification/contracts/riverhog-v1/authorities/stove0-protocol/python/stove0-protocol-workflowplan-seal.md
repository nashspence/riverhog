# stove0_protocol.WorkflowPlan.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplan-seal:31bd4fa514 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ca449b887"></a>
| Field | Shape |
|---|---|
| <a id="s-8e13e8c692"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c94cefd9e4"></a>`distribution` | "stove0-protocol" |
| <a id="s-adb7b62f68"></a>`module` | "stove0_protocol" |
| <a id="s-f86eb8de8f"></a>`name` | "seal" |
| <a id="s-fcd7319abe"></a>`owner` | "stove0_protocol.WorkflowPlan" |
| <a id="s-04b6fb0324"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPlan](stove0-protocol-workflowplan.md)

## Governing policies

- <a id="pa-042acc72e1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlan.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2fe97ec77dd59b75b5cf5d963ef6b287b3d25d5b83ec13cce6f931619a59f53c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'WorkflowPlanPayload') -> 'WorkflowPlan'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.WorkflowPlan",
  "unit": "member"
}
```
