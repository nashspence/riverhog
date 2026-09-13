# stove0_observer_protocol.ObservationInapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationinapplicable:63c9ffc85e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b0c0d0b131"></a>
| Field | Shape |
|---|---|
| <a id="s-4eec831bc6"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-a687240396"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-1ec0f4aee6"></a>`module` | "stove0_observer_protocol" |
| <a id="s-02b321d654"></a>`name` | "ObservationInapplicable" |
| <a id="s-d12a213119"></a>`unit` | "export" |

## Governing policies

- <a id="pa-61d7ab9f83"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationInapplicable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6637e614a132c2c16e8737b7c52d52a97bbaed71092019d1c2f58f72aa853b6f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "95bfa40604f4246ad72c306da05ea43a9d657940bcf1ec5706fe8073e81748bd",
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationInapplicable",
  "unit": "export"
}
```
