# stove0_observer_protocol.ARTIFACT_ID_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-artifact-id-pattern:2c9e73e375 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5954d62b61"></a>
| Field | Shape |
|---|---|
| <a id="s-8ae1cab0ee"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-982adaebc5"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-2a08eddd99"></a>`module` | "stove0_observer_protocol" |
| <a id="s-da6c3a5aad"></a>`name` | "ARTIFACT_ID_PATTERN" |
| <a id="s-4c2a16cf95"></a>`unit` | "export" |

## Governing policies

- <a id="pa-66b085f1d0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ARTIFACT_ID_PATTERN`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8491de8f0551b700c34f148e48090218016ad269cd5e208f194dde7e6b78150c -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ARTIFACT_ID_PATTERN",
  "unit": "export"
}
```
