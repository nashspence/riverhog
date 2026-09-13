# stove0_observer_protocol.RIVERHOG_CAPABILITY_TRANSPORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-riverhog-capabil-1128d4ef82:c3d03dafda -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5111571e6e"></a>
| Field | Shape |
|---|---|
| <a id="s-30e37e427b"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-ec14a05968"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-d4b3403148"></a>`module` | "stove0_observer_protocol" |
| <a id="s-c47b8aa4d5"></a>`name` | "RIVERHOG_CAPABILITY_TRANSPORT" |
| <a id="s-4c77cb1b12"></a>`unit` | "export" |

## Governing policies

- <a id="pa-168909fc14"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.RIVERHOG_CAPABILITY_TRANSPORT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c10445796e0b1e42b2e3b35768de5b16c97836dd797fe27edeecefa15131600d -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-capability/v1"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "RIVERHOG_CAPABILITY_TRANSPORT",
  "unit": "export"
}
```
