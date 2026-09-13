# stove0_protocol.WorkflowPreviewPayload.canonical_observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload-ca-43137b7b1f:8d0bc7a044 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4053064134"></a>
| Field | Shape |
|---|---|
| <a id="s-468c2a7b22"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3e5bbe9c1c"></a>`distribution` | "stove0-protocol" |
| <a id="s-3203c0cfd5"></a>`module` | "stove0_protocol" |
| <a id="s-d5d4a251e5"></a>`name` | "canonical_observations" |
| <a id="s-46a6d3312f"></a>`owner` | "stove0_protocol.WorkflowPreviewPayload" |
| <a id="s-5bdfd841ad"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPreviewPayload](stove0-protocol-workflowpreviewpayload.md)

## Governing policies

- <a id="pa-bd61bc68b7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload.canonical_observations`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4be19f2ceaa109b07a47c87948577bf737488a45e69dc49084572b79109f1483 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_observations",
  "owner": "stove0_protocol.WorkflowPreviewPayload",
  "unit": "member"
}
```
