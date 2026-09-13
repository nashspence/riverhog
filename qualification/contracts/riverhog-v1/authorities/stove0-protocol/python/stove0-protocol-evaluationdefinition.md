# stove0_protocol.EvaluationDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationdefinition:89c046ee44 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1937071e44"></a>
| Field | Shape |
|---|---|
| <a id="s-b87e1fc1d1"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-eab39ec1e6"></a>`distribution` | "stove0-protocol" |
| <a id="s-1c3a591e24"></a>`module` | "stove0_protocol" |
| <a id="s-d4bfa72105"></a>`name` | "EvaluationDefinition" |
| <a id="s-37e10fc1cd"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationDefinition.child_work](stove0-protocol-evaluationdefinition-child-work.md)
- [stove0_protocol.EvaluationDefinition.child_works](stove0-protocol-evaluationdefinition-child-works.md)
- [stove0_protocol.EvaluationDefinition.seal](stove0-protocol-evaluationdefinition-seal.md)
- [stove0_protocol.EvaluationDefinition.verify_digest](stove0-protocol-evaluationdefinition-verify-digest.md)

## Governing policies

- <a id="pa-909885833a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationDefinition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d36989787991a0a3704b7a09211011a7528aaf296983d76535c5323b83da144 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "73dde42300b8d2864bca552e753011ebc0e62eac66f6181892291fd37f0e7977",
    "signature": "\"(*, format: Literal['stove0-evaluation-definition/v1'] = 'stove0-evaluation-definition/v1', purpose: Literal['trial', 'evaluation'] = 'evaluation', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], common_intent: dict[str, JsonValue] = <factory>, matrix: stove0_protocol.models.EvaluationMatrix, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationDefinition",
  "unit": "export"
}
```
