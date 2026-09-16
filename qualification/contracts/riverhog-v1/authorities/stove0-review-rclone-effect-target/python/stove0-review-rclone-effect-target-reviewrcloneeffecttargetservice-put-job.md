# stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-review-3e15080198:d352cf292e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-347fd24619"></a>
- <a id="s-0da698ec8a"></a>`distribution`: `stove0-review-rclone-effect-target`
- <a id="s-4f25eb31c1"></a>`module`: `stove0_review_rclone_effect_target`
- <a id="s-1a830b2d2a"></a>`name`: `put_job`
- <a id="s-7195224fbd"></a>`owner`: `stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService`
- <a id="s-5a4d1145a2"></a>`unit`: `member`

### Declared structure

- <a id="s-f6244e773c"></a>`kind`: `"method"`
- <a id="s-abf75ac178"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-ae1b09aa37"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources.md#src-5fd1cb5bbe) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f2db77ed2322bbf9d30cbcfbb400ca83f4da7ccdcdc09294f8f516c4972698a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-review-rclone-effect-target",
  "module": "stove0_review_rclone_effect_target",
  "name": "put_job",
  "owner": "stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
