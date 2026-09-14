# stove0_review_planning.evenly_spaced_sample_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-planning:stove0-review-planning-evenly-spaced-sample-plan:997f02497a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-315740d1d6"></a>
- <a id="s-eeef7fa97c"></a>`distribution`: `stove0-review-planning`
- <a id="s-2c75331680"></a>`module`: `stove0_review_planning`
- <a id="s-313432920c"></a>`name`: `evenly_spaced_sample_plan`
- <a id="s-91cf72b4eb"></a>`unit`: `export`

### Declared structure

- <a id="s-56443cf034"></a>`kind`: `"function"`
- <a id="s-7f089b0ea2"></a>`signature`: `"\"(facts: 'MediaSamplingFacts', *, samples_per_artifact: 'int', window_duration_ms: 'int') -> 'ReviewSamplePlan'\""`

## Governing policies

- <a id="pa-24b5673e4b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-planning:stove0_review_planning](../../../evidence/sources.md#src-354ae519e9) — `reference/stove0/targets/review/planning/src/stove0_review_planning/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_planning.evenly_spaced_sample_plan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6ab1a819ee7b3f86c1e35b8b5d8cde7483f5a71d55129b10fd0088f4190068e -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(facts: 'MediaSamplingFacts', *, samples_per_artifact: 'int', window_duration_ms: 'int') -> 'ReviewSamplePlan'\""
  },
  "distribution": "stove0-review-planning",
  "module": "stove0_review_planning",
  "name": "evenly_spaced_sample_plan",
  "unit": "export"
}
```
