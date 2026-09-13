# stove0_observer_protocol.ObserverContractPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontractpayload:c3324347ca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aa2f1bb0b5"></a>
| Field | Shape |
|---|---|
| <a id="s-4f649a7e35"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5229a6af7c"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-87857832c4"></a>`module` | "stove0_observer_protocol" |
| <a id="s-642e654a0e"></a>`name` | "ObserverContractPayload" |
| <a id="s-b3f88d0044"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObserverContractPayload.bind_semantic_conformance_vectors](stove0-observer-protocol-observercontractpayload-bind-semantic-conformance-vectors.md)

## Governing policies

- <a id="pa-3992f548ba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContractPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c5dcf33f4c614fabe3d1ba7ecaf2560a575e70ee7c8c532c46a38c72a4bf284 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ed174398a0f2af336d7aa0f8dee516b6c7bbc8738fc13f26411e98bcb9f66df0",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverContractPayload",
  "unit": "export"
}
```
