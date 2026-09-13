# stove0_observer_protocol.SemanticFactsConformanceVector.canonical_subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticfactscon-0e14778ca4:2d492cd22a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f4e78d5eb0"></a>
| Field | Shape |
|---|---|
| <a id="s-8a03fe47c5"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d65778bdc4"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-a2965212f1"></a>`module` | "stove0_observer_protocol" |
| <a id="s-3baf9932ca"></a>`name` | "canonical_subjects" |
| <a id="s-152afeb30c"></a>`owner` | "stove0_observer_protocol.SemanticFactsConformanceVector" |
| <a id="s-44c5b96081"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticFactsConformanceVector](stove0-observer-protocol-semanticfactsconformancevector.md)

## Governing policies

- <a id="pa-8be499de5b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticFactsConformanceVector.canonical_subjects`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c548156a5d38992c0ffca39a8e08ddb353e98f1f657c5f9eb4b156672440aca -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_subjects",
  "owner": "stove0_observer_protocol.SemanticFactsConformanceVector",
  "unit": "member"
}
```
