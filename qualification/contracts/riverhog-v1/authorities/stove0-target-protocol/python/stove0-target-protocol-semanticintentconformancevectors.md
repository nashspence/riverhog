# stove0_target_protocol.SemanticIntentConformanceVectors

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticintentconf-ae4a14ed7c:f3d4992823 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76799b2f82"></a>
| Field | Shape |
|---|---|
| <a id="s-32a5659de7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-48b84baae6"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-ea0336bb8f"></a>`module` | "stove0_target_protocol" |
| <a id="s-502a05ea08"></a>`name` | "SemanticIntentConformanceVectors" |
| <a id="s-b42046ea6b"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.SemanticIntentConformanceVectors.covers_acceptance_and_rejection](stove0-target-protocol-semanticintentconformancevectors-covers-acceptance-and-rejection.md)
- [stove0_target_protocol.SemanticIntentConformanceVectors.canonical_vectors](stove0-target-protocol-semanticintentconformancevectors-canonical-vectors.md)
- [stove0_target_protocol.SemanticIntentConformanceVectors.sha256](stove0-target-protocol-semanticintentconformancevectors-sha256.md)

## Governing policies

- <a id="pa-8a9b5b350e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticIntentConformanceVectors`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34027056a49bdf143700eb797f046779874cbe3d644e12d49c72244166aa6590 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "97bacfee1ce36f2e715ebe95a8f92c7553faaeda88e9080ae8b07a33a04165b0",
    "signature": "\"(*, format: Literal['stove0-semantic-intent-conformance/v1'] = 'stove0-semantic-intent-conformance/v1', profile_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], vectors: Annotated[tuple[stove0_target_protocol.conformance.SemanticIntentConformanceVector, ...], MinLen(min_length=2)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "SemanticIntentConformanceVectors",
  "unit": "export"
}
```
