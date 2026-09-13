# stove0_observer_protocol.ObservationState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationstate:00d9274207 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ae9b539a8f"></a>
| Field | Shape |
|---|---|
| <a id="s-be2158c084"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-f51169d126"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-dd0100f08d"></a>`module` | "stove0_observer_protocol" |
| <a id="s-776b5716a7"></a>`name` | "ObservationState" |
| <a id="s-341036ea83"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f8a1fffdd0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationState`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f9e3d008db7702224f27fd65193e8f9be2699305646bee805dc001f44642116 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationState",
  "unit": "export"
}
```
