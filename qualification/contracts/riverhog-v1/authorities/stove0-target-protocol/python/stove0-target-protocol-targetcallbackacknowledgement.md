# stove0_target_protocol.TargetCallbackAcknowledgement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcallbackacknowledgement:7e07d79cd2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63e29c9597"></a>
| Field | Shape |
|---|---|
| <a id="s-fa04bd1960"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-b3df149366"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-1494aa5b74"></a>`module` | "stove0_target_protocol" |
| <a id="s-eacab8333a"></a>`name` | "TargetCallbackAcknowledgement" |
| <a id="s-42e2d9e38b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8bedfac872"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetCallbackAcknowledgement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3dacb85bd9f7d3451a823d80d47ed1b0212ebab6cb2967d0c56dc899308872e5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "888aeb6259ebbfb946fb389f43530f2e7f42b0aaa47127fc93cde9b46c06ccfc",
    "signature": "'(*, accepted: Literal[True] = True) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetCallbackAcknowledgement",
  "unit": "export"
}
```
