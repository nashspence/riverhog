# stove0_observer_protocol.ObservationFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationfailure:b00f7e3eae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d70c3458b6"></a>
| Field | Shape |
|---|---|
| <a id="s-402893ca1b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ee568c2907"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-bb4d7ca89c"></a>`module` | "stove0_observer_protocol" |
| <a id="s-eadbb1d0ec"></a>`name` | "ObservationFailure" |
| <a id="s-f481c14a43"></a>`unit` | "export" |

## Governing policies

- <a id="pa-94aadb3667"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationFailure`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5e2c34777905bc505adbfd56211fcf0fc7b357922ffc56570d4e965bec53b5b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "03452b85273ec047f313b026f245c633c5677b66c3c74316258933f63c051c7f",
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationFailure",
  "unit": "export"
}
```
