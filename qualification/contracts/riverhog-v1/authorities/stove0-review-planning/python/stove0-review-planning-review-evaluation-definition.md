# stove0_review_planning.review_evaluation_definition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-planning:stove0-review-planning-review-evaluation-definition:57f03100a0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9ed5ffaf3"></a>
| Field | Shape |
|---|---|
| <a id="s-8c16c0740f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f4605ba624"></a>`distribution` | "stove0-review-planning" |
| <a id="s-8d1b9d95d5"></a>`module` | "stove0_review_planning" |
| <a id="s-9a5ab8fc81"></a>`name` | "review_evaluation_definition" |
| <a id="s-c0d6d6657e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f08b44c07c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-planning:stove0_review_planning](../../../evidence/sources.md#src-354ae519e9) — `reference/stove0/targets/review/planning/src/stove0_review_planning/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_planning.review_evaluation_definition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89ad4020d2849e07af89412d5afdfe73fb4c0bd24fa05a6ef2daab9510d0e6d0 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "'(*, recipe: \\'RecipeRef\\', inputs: \\'tuple[CollectionRootRef, ...]\\', sample_plan: \\'ReviewSamplePlan\\', variants: \\'tuple[ReviewVariant, ...]\\', purpose: \"Literal[\\'trial\\', \\'evaluation\\']\" = \\'evaluation\\', common_intent: \\'dict[str, JsonValue] | None\\' = None) -> \\'EvaluationDefinition\\''"
  },
  "distribution": "stove0-review-planning",
  "module": "stove0_review_planning",
  "name": "review_evaluation_definition",
  "unit": "export"
}
```
