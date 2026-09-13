# stove0_target_protocol.TargetJobStatus.validate_terminal_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobstatus-va-c9d6efc4ed:487c32c659 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e8a2552522"></a>
| Field | Shape |
|---|---|
| <a id="s-07345733b7"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c8c7112b77"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-97ca4ee757"></a>`module` | "stove0_target_protocol" |
| <a id="s-50d0194c7f"></a>`name` | "validate_terminal_shape" |
| <a id="s-badcf40926"></a>`owner` | "stove0_target_protocol.TargetJobStatus" |
| <a id="s-ed828b72b6"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetJobStatus](stove0-target-protocol-targetjobstatus.md)

## Governing policies

- <a id="pa-5d414bce9c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobStatus.validate_terminal_shape`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be25e15a00063ebe13be13e97384017fc61a5b895fd2311f6baa69028b1d5078 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_terminal_shape",
  "owner": "stove0_target_protocol.TargetJobStatus",
  "unit": "member"
}
```
