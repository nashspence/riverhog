# stove0_observer_protocol.ObserverContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontract:7f8d7fbee0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-085164d5b9"></a>
| Field | Shape |
|---|---|
| <a id="s-a5d65eca1d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7b80c6fc11"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-afdbacd651"></a>`module` | "stove0_observer_protocol" |
| <a id="s-a319b4b75c"></a>`name` | "ObserverContract" |
| <a id="s-c0dccc94bf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObserverContract.verify_digest](stove0-observer-protocol-observercontract-verify-digest.md)
- [stove0_observer_protocol.ObserverContract.seal](stove0-observer-protocol-observercontract-seal.md)

## Governing policies

- <a id="pa-a7d8c4c37e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75b490e2474b59b52b286f272551e961f9269a67e43eed2313b622be7c56cb0b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "79326d184bed03d400708fc7fca5dca34ea84a3c02cb3e536cd3788f1daf1ff7",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverContract",
  "unit": "export"
}
```
