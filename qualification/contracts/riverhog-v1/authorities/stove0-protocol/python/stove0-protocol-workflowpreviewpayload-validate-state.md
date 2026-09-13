# stove0_protocol.WorkflowPreviewPayload.validate_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload-va-d6966cab70:08c727f1c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e0a85e825"></a>
| Field | Shape |
|---|---|
| <a id="s-90e0bfd921"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-403a673150"></a>`distribution` | "stove0-protocol" |
| <a id="s-d4abde9715"></a>`module` | "stove0_protocol" |
| <a id="s-8f710917f7"></a>`name` | "validate_state" |
| <a id="s-1dc14d9864"></a>`owner` | "stove0_protocol.WorkflowPreviewPayload" |
| <a id="s-6b18a8b327"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPreviewPayload](stove0-protocol-workflowpreviewpayload.md)

## Governing policies

- <a id="pa-d00ba88845"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload.validate_state`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6dde4ee005bd97cbad771a1f59d052163fe86091f6dbf4d2352829d19d8b2185 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "validate_state",
  "owner": "stove0_protocol.WorkflowPreviewPayload",
  "unit": "member"
}
```
