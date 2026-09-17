# stove0_review_target_support.ReviewTargetServiceBase.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtarget-8c82fef051:7ee0d4e548 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0018ff5f16"></a>
- <a id="s-aec3576541"></a>`distribution`: `stove0-review-target-support`
- <a id="s-788d973235"></a>`module`: `stove0_review_target_support`
- <a id="s-d01cfa6ed1"></a>`name`: `put_job`
- <a id="s-d968fcbb92"></a>`owner`: `stove0_review_target_support.ReviewTargetServiceBase`
- <a id="s-46899d0efd"></a>`unit`: `member`

### Declared structure

- <a id="s-795e7d9399"></a>`kind`: `"method"`
- <a id="s-1d8ee13b75"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](stove0-review-target-support-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-1c476da723"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources/authorities.md#src-2a89a71c41) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9210e2f1a3274bdcae33fd0b8852322d3ba43c9eb8bb9c847f56768201b437d9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "put_job",
  "owner": "stove0_review_target_support.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
