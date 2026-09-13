# stove0_observer_protocol.ObservationResult.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationresul-3a6bc3d3de:e5073db4af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ea33f91ac"></a>
| Field | Shape |
|---|---|
| <a id="s-d14dfd78ae"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4804b09726"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-6d7ada9a21"></a>`module` | "stove0_observer_protocol" |
| <a id="s-2167433a46"></a>`name` | "verify_digest" |
| <a id="s-4f79c694b6"></a>`owner` | "stove0_observer_protocol.ObservationResult" |
| <a id="s-88725ace85"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObservationResult](stove0-observer-protocol-observationresult.md)

## Governing policies

- <a id="pa-13ca68641a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationResult.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e87e1b6f305fa4031fe59f7a5a50d795e1135e87f2d2eda1aa19179b8af0fb2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "verify_digest",
  "owner": "stove0_observer_protocol.ObservationResult",
  "unit": "member"
}
```
