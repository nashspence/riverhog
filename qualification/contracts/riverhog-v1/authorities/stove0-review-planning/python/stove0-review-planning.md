# stove0_review_planning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-planning:stove0-review-planning:8f3625e956 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-9d62849007) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ad1e2bae3d"></a>
| Field | Shape |
|---|---|
| <a id="s-8ca8e29e1c"></a>`candidate_id` | "python:stove0-review-planning:stove0_review_planning" |
| <a id="s-3c28f6a153"></a>`distribution` | "stove0-review-planning" |
| <a id="s-1f3892dd70"></a>`exports` | additional keys=`ReviewVariant`, `contract_report`, `evenly_spaced_sample_plan`, `review_evaluation_definition` |
| <a id="s-5d2cfd5f56"></a>`module` | "stove0_review_planning" |

## Governing policies

- <a id="pa-c126190a96"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-planning:stove0_review_planning](../../../evidence/sources.md#src-354ae519e9) — `reference/stove0/targets/review/planning/src/stove0_review_planning/__init__.py`

### Machine authority

- `/external_contract/python/50`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a6d8f6541e9b6fd5f61530112524c44615a375aec82f411d6dab268baaccca5c -->

```json
{
  "candidate_id": "python:stove0-review-planning:stove0_review_planning",
  "distribution": "stove0-review-planning",
  "exports": {
    "ReviewVariant": {
      "kind": "class",
      "schema_sha256": "4d9480938840f87d7b1881952c04480a5d339ff1a809971d9445974dcc07287b",
      "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>) -> None\""
    },
    "contract_report": {
      "kind": "function",
      "signature": "\"() -> 'dict[str, object]'\""
    },
    "evenly_spaced_sample_plan": {
      "kind": "function",
      "signature": "\"(facts: 'MediaSamplingFacts', *, samples_per_artifact: 'int', window_duration_ms: 'int') -> 'ReviewSamplePlan'\""
    },
    "review_evaluation_definition": {
      "kind": "function",
      "signature": "'(*, recipe: \\'RecipeRef\\', inputs: \\'tuple[CollectionRootRef, ...]\\', sample_plan: \\'ReviewSamplePlan\\', variants: \\'tuple[ReviewVariant, ...]\\', purpose: \"Literal[\\'trial\\', \\'evaluation\\']\" = \\'evaluation\\', common_intent: \\'dict[str, JsonValue] | None\\' = None) -> \\'EvaluationDefinition\\''"
    }
  },
  "module": "stove0_review_planning"
}
```
