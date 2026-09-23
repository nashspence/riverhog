# review0_planner.evenly_spaced_sample_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-planner:review0-planner-evenly-spaced-sample-plan:bdab192a08 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-planner](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46173afdf2"></a>
- <a id="s-75aed7103b"></a>`distribution`: `review0-planner`
- <a id="s-a479c9785c"></a>`module`: `review0_planner`
- <a id="s-1a917124a2"></a>`name`: `evenly_spaced_sample_plan`
- <a id="s-465b77e7eb"></a>`unit`: `export`

### Declared structure

- <a id="s-a64984adca"></a>`kind`: `"function"`
- <a id="s-5dc984f7a8"></a>`signature`: `"\"(facts: 'MediaSamplingFacts', *, samples_per_artifact: 'int', window_duration_ms: 'int') -> 'ReviewSamplePlan'\""`

## Governing policies

- <a id="pa-3ac2f40998"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-planner:review0_planner](../../../evidence/sources/authorities.md#src-a8a843c072) — [some-implementations/stove0/review0/planning/src/review0\_planner/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/planning/src/review0_planner/__init__.py)

### Machine authority

- `/external_contract/python/review0_planner.evenly_spaced_sample_plan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c2a43a8cfa58c66b8d2a8cd2d8b9a00cd75508a701178ea05c58ea2677bad165 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(facts: 'MediaSamplingFacts', *, samples_per_artifact: 'int', window_duration_ms: 'int') -> 'ReviewSamplePlan'\""
  },
  "distribution": "review0-planner",
  "module": "review0_planner",
  "name": "evenly_spaced_sample_plan",
  "unit": "export"
}
```

</details>
