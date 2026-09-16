# stove0_review_target_support.ReviewTargetServiceBase.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtarget-680e845cbc:1cad480ca6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8328028612"></a>
- <a id="s-174cd43e62"></a>`distribution`: `stove0-review-target-support`
- <a id="s-cfd17d2634"></a>`module`: `stove0_review_target_support`
- <a id="s-d70f0853a1"></a>`name`: `cancel_job`
- <a id="s-91ff6a15fd"></a>`owner`: `stove0_review_target_support.ReviewTargetServiceBase`
- <a id="s-bd10236d65"></a>`unit`: `member`

### Declared structure

- <a id="s-8aadd40498"></a>`kind`: `"method"`
- <a id="s-9523373297"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](stove0-review-target-support-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-52a9380f77"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase.cancel_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4dfd0247ab97d26ca947b46c1b0f112c4efd0cba31e7a1962b890519bb944c0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "cancel_job",
  "owner": "stove0_review_target_support.ReviewTargetServiceBase",
  "unit": "member"
}
```
