# stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-review-9b80216a16:d1857fccd1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2788efe388"></a>
- <a id="s-5802d69d4d"></a>`distribution`: `stove0-review-rclone-effect-target`
- <a id="s-b3993efbb6"></a>`module`: `stove0_review_rclone_effect_target`
- <a id="s-d9841b1e37"></a>`name`: `cancel_job`
- <a id="s-01bd71904d"></a>`owner`: `stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService`
- <a id="s-1aaae81c77"></a>`unit`: `member`

### Declared structure

- <a id="s-5d8f89e716"></a>`kind`: `"method"`
- <a id="s-cd7ccbec60"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-d178deac18"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources.md#src-5fd1cb5bbe) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.cancel_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80a87d117be3c8e5ef8a45460a26c63211c797be58809ac1869d4566a895cda5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-review-rclone-effect-target",
  "module": "stove0_review_rclone_effect_target",
  "name": "cancel_job",
  "owner": "stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
