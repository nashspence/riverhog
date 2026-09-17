# stove0_review_planning.review_evaluation_definition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-planning:stove0-review-planning-review-evaluation-definition:57f03100a0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9ed5ffaf3"></a>
- <a id="s-f4605ba624"></a>`distribution`: `stove0-review-planning`
- <a id="s-8d1b9d95d5"></a>`module`: `stove0_review_planning`
- <a id="s-9a5ab8fc81"></a>`name`: `review_evaluation_definition`
- <a id="s-c0d6d6657e"></a>`unit`: `export`

### Declared structure

- <a id="s-41812a7627"></a>`kind`: `"function"`
- <a id="s-092ac17e88"></a>`signature`: `"'(*, recipe: \\'RecipeRef\\', inputs: \\'tuple[CollectionRootRef, ...]\\', sample_plan: \\'ReviewSamplePlan\\', variants: \\'tuple[ReviewVariant, ...]\\', purpose: \"Literal[\\'trial\\', \\'evaluation\\']\" = \\'evaluation\\', common_intent: \\'dict[str, JsonValue] \| None\\' = None) -> \\'EvaluationDefinition\\''"`

## Governing policies

- <a id="pa-f08b44c07c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-planning:stove0_review_planning](../../../evidence/sources/authorities.md#src-354ae519e9) — [reference/stove0/targets/review/planning/src/stove0\_review\_planning/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/planning/src/stove0_review_planning/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_planning.review_evaluation_definition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
