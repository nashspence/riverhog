# stove0_review_target_support.ReviewTargetServiceBase.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtarget-8b536d0d32:08c0d340d5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ca6eb41e54"></a>
- <a id="s-a03c702478"></a>`distribution`: `stove0-review-target-support`
- <a id="s-b4cbf707ef"></a>`module`: `stove0_review_target_support`
- <a id="s-d120eef7c0"></a>`name`: `get_job`
- <a id="s-ef0cc8ef5f"></a>`owner`: `stove0_review_target_support.ReviewTargetServiceBase`
- <a id="s-e326d3a70a"></a>`unit`: `member`

### Declared structure

- <a id="s-5a7c3465ee"></a>`kind`: `"method"`
- <a id="s-a8499f1517"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](stove0-review-target-support-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-e484513a64"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase.get_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5acafca34beea26007da2531b977a7437c0de1046ca4a9487c28409882530614 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "get_job",
  "owner": "stove0_review_target_support.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
