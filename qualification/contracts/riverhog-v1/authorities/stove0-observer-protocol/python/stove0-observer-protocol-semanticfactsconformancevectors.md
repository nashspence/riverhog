# stove0_observer_protocol.SemanticFactsConformanceVectors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticfactscon-2aaf5975ac:2de32a41d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b0e56a5bc"></a>
| Field | Shape |
|---|---|
| <a id="s-e5ba944529"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-661303850d"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-e7b2c82cbd"></a>`module` | "stove0_observer_protocol" |
| <a id="s-d849e7f95c"></a>`name` | "SemanticFactsConformanceVectors" |
| <a id="s-67f974ee4e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticFactsConformanceVectors.canonical_vectors](stove0-observer-protocol-semanticfactsconformancevectors-canonical-vectors.md)
- [stove0_observer_protocol.SemanticFactsConformanceVectors.sha256](stove0-observer-protocol-semanticfactsconformancevectors-sha256.md)
- [stove0_observer_protocol.SemanticFactsConformanceVectors.covers_acceptance_and_rejection](stove0-observer-protocol-semanticfactsconformancevectors-covers-acceptance-and-rejection.md)

## Governing policies

- <a id="pa-d7489868b1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticFactsConformanceVectors`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1a1999f02cc22b87707692ad7a7ce0c80b640eb9c898a6215ef08ae70824a11 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "fa3edfab36bf499bb8091bc692ce8b2b46628ada7c9db518458ffd8f02fb1aa7",
    "signature": "\"(*, format: Literal['stove0-semantic-facts-conformance/v1'] = 'stove0-semantic-facts-conformance/v1', profile_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], vectors: Annotated[tuple[stove0_observer_protocol.conformance.SemanticFactsConformanceVector, ...], MinLen(min_length=2)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SemanticFactsConformanceVectors",
  "unit": "export"
}
```
