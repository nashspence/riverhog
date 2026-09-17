# stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-review-b101650267:267e14d6da -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5afa6369d3"></a>
- <a id="s-1202f8f710"></a>`distribution`: `stove0-review-rclone-effect-target`
- <a id="s-e06276fac6"></a>`module`: `stove0_review_rclone_effect_target`
- <a id="s-84f6cb546d"></a>`name`: `get_job`
- <a id="s-6799612f73"></a>`owner`: `stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService`
- <a id="s-d657f19227"></a>`unit`: `member`

### Declared structure

- <a id="s-c7b5acdbda"></a>`kind`: `"method"`
- <a id="s-248013adae"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-ce33559ea2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources/authorities.md#src-5fd1cb5bbe) — [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.get_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02eb08eb46470e007e0b28b18fa1f5d7ca6b78c9a4b458bbdc0cd9953b7d1998 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-review-rclone-effect-target",
  "module": "stove0_review_rclone_effect_target",
  "name": "get_job",
  "owner": "stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
