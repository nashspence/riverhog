# stove0_protocol.JoinPlan.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinplan-canonical-inputs:967101e64d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e056f53f44"></a>
- <a id="s-22e4cfe5d5"></a>`distribution`: `stove0-protocol`
- <a id="s-1491136702"></a>`module`: `stove0_protocol`
- <a id="s-6476f98bd3"></a>`name`: `canonical_inputs`
- <a id="s-9def46b343"></a>`owner`: `stove0_protocol.JoinPlan`
- <a id="s-8d822c8da0"></a>`unit`: `member`

### Declared structure

- <a id="s-462493112d"></a>`kind`: `"classmethod"`
- <a id="s-641889a3ca"></a>`signature`: `"\"(cls, value: 'tuple[JoinInputPlan, ...]') -> 'tuple[JoinInputPlan, ...]'\""`

## Maintained corroboration

### Related interface records

- [JoinPlan](stove0-protocol-joinplan.md)

## Governing policies

- <a id="pa-d79263cb54"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinPlan.canonical_inputs`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ac3a74d4f635686c8c4eeac1620c242856b61e6480c0be155fe9eacf21cec90 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[JoinInputPlan, ...]') -> 'tuple[JoinInputPlan, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_inputs",
  "owner": "stove0_protocol.JoinPlan",
  "unit": "member"
}
```
