# review0_planner.review_evaluation_definition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-planner:review0-planner-review-evaluation-definition:7d34c09080 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-planner](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f370f164b5"></a>
- <a id="s-afedddfb4d"></a>`distribution`: `review0-planner`
- <a id="s-31e6e17f76"></a>`module`: `review0_planner`
- <a id="s-b786ce4ab8"></a>`name`: `review_evaluation_definition`
- <a id="s-befa0313e8"></a>`unit`: `export`

### Declared structure

- <a id="s-bcca25bdcc"></a>`kind`: `"function"`
- <a id="s-4a46778728"></a>`signature`: `"'(*, recipe: \\'RecipeIdentityRef\\', inputs: \\'tuple[CollectionRootIdentityRef, ...]\\', sample_plan: \\'ReviewSamplePlan\\', variants: \\'tuple[ReviewVariant, ...]\\', purpose: \"Literal[\\'trial\\', \\'evaluation\\']\" = \\'evaluation\\', common_intent: \\'dict[str, JsonValue] \| None\\' = None) -> \\'EvaluationDefinition\\''"`

## Governing policies

- <a id="pa-733435f0ea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-planner:review0_planner](../../../evidence/sources/authorities.md#src-a8a843c072) — [some-implementations/stove0/review0/planning/src/review0\_planner/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/planning/src/review0_planner/__init__.py)

### Machine authority

- `/external_contract/python/review0_planner.review_evaluation_definition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf2a0b15df9f61f079a00367a33aa4b703294d4413554487d3c8f9821f894049 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "'(*, recipe: \\'RecipeIdentityRef\\', inputs: \\'tuple[CollectionRootIdentityRef, ...]\\', sample_plan: \\'ReviewSamplePlan\\', variants: \\'tuple[ReviewVariant, ...]\\', purpose: \"Literal[\\'trial\\', \\'evaluation\\']\" = \\'evaluation\\', common_intent: \\'dict[str, JsonValue] | None\\' = None) -> \\'EvaluationDefinition\\''"
  },
  "distribution": "review0-planner",
  "module": "review0_planner",
  "name": "review_evaluation_definition",
  "unit": "export"
}
```

</details>
