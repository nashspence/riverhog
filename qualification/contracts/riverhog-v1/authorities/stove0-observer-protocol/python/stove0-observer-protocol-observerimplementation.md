# stove0_observer_protocol.ObserverImplementation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerimplementation:5d6c0c7b94 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ffe2132b9"></a>
| Field | Shape |
|---|---|
| <a id="s-e988bee1e1"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-a96b49937e"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-bd8cd92316"></a>`module` | "stove0_observer_protocol" |
| <a id="s-1cca12b586"></a>`name` | "ObserverImplementation" |
| <a id="s-05b7fff3b6"></a>`unit` | "export" |

## Governing policies

- <a id="pa-5f034f88e3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverImplementation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 034e2f62cf562d3ff47895c21fead8cb13a8f61576542b9dde11a511b56c29e3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6ffdaa644c4d7b3d7f3e17d66ebec04a5997be6e6e59c8839ae0685b1250ba75",
    "signature": "\"(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverImplementation",
  "unit": "export"
}
```
