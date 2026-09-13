# stove0_protocol.EvaluationMatrixPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationmatrixpayload:b3db29a0e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-589662a388"></a>
| Field | Shape |
|---|---|
| <a id="s-4fa63e406f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-91516b4bec"></a>`distribution` | "stove0-protocol" |
| <a id="s-e5f5987ede"></a>`module` | "stove0_protocol" |
| <a id="s-3b1c9676b7"></a>`name` | "EvaluationMatrixPayload" |
| <a id="s-3cf1f5b531"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationMatrixPayload.canonical_variants](stove0-protocol-evaluationmatrixpayload-canonical-variants.md)

## Governing policies

- <a id="pa-0e2d6fc81f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationMatrixPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd4513f4c3b5fc084aad7ffd94e85234e6d1b903414553207351207332d12157 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "f729430d8ae4e5c0ba3f436617b5f23f042b6dc238ae5f68a8b5b035ff853614",
    "signature": "\"(*, format: Literal['stove0-evaluation-matrix/v1'] = 'stove0-evaluation-matrix/v1', variants: Annotated[tuple[stove0_protocol.models.EvaluationVariant, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationMatrixPayload",
  "unit": "export"
}
```
