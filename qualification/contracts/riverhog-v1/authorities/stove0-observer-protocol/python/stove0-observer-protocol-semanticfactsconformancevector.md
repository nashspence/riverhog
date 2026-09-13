# stove0_observer_protocol.SemanticFactsConformanceVector

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticfactscon-717782a348:9f2fafb6e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b914ba133"></a>
| Field | Shape |
|---|---|
| <a id="s-13cd41dc18"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0880404436"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-d5628b0355"></a>`module` | "stove0_observer_protocol" |
| <a id="s-5cb0276e5c"></a>`name` | "SemanticFactsConformanceVector" |
| <a id="s-bcad0aaad8"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticFactsConformanceVector.canonical_subjects](stove0-observer-protocol-semanticfactsconformancevector-canonical-subjects.md)

## Governing policies

- <a id="pa-962f99ea2a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticFactsConformanceVector`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e845b9de4b3b6c12553f7aafd3b8c05f4317ff9385ee79ab69ddbd230197ecab -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7d466116d6fc7df4a6299063bc8a4f2729dc8471c4f7a0144f7da4a250c34ba3",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], accepted: bool, subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, facts: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SemanticFactsConformanceVector",
  "unit": "export"
}
```
