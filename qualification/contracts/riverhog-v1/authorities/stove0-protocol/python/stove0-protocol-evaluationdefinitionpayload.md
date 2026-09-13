# stove0_protocol.EvaluationDefinitionPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluationdefinitionpayload:7841f227f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6616483d80"></a>
| Field | Shape |
|---|---|
| <a id="s-761416a7c2"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-70b01cddbd"></a>`distribution` | "stove0-protocol" |
| <a id="s-5cb40653f2"></a>`module` | "stove0_protocol" |
| <a id="s-4b9fb49410"></a>`name` | "EvaluationDefinitionPayload" |
| <a id="s-11e2c6cc95"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.EvaluationDefinitionPayload.canonical_inputs](stove0-protocol-evaluationdefinitionpayload-canonical-inputs.md)
- [stove0_protocol.EvaluationDefinitionPayload.validate_purpose](stove0-protocol-evaluationdefinitionpayload-validate-purpose.md)

## Governing policies

- <a id="pa-b4b6a6b4e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.EvaluationDefinitionPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8128b501c273ef4f44e02b59ded59d490af31303aa995009cf38d3ee650fba0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c07e0127adc95a58e9faaa49fce7798075248e5b13b49b762ae9f37be3d4f62a",
    "signature": "\"(*, format: Literal['stove0-evaluation-definition/v1'] = 'stove0-evaluation-definition/v1', purpose: Literal['trial', 'evaluation'] = 'evaluation', recipe: stove0_protocol.models.RecipeRef, inputs: Annotated[tuple[stove0_protocol.models.CollectionRootRef, ...], MinLen(min_length=1)], common_intent: dict[str, JsonValue] = <factory>, matrix: stove0_protocol.models.EvaluationMatrix) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EvaluationDefinitionPayload",
  "unit": "export"
}
```
