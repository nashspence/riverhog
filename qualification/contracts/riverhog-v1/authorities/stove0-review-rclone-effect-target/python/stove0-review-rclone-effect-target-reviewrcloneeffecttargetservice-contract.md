# stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-review-5e83cd1007:bc05743d95 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76ae4c26e2"></a>
- <a id="s-d9d82f676c"></a>`distribution`: `stove0-review-rclone-effect-target`
- <a id="s-4ca926825d"></a>`module`: `stove0_review_rclone_effect_target`
- <a id="s-9fc632bc6a"></a>`name`: `contract`
- <a id="s-c69dfa63d3"></a>`owner`: `stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService`
- <a id="s-d824769b0b"></a>`unit`: `member`

### Declared structure

- <a id="s-8831cc7712"></a>`kind`: `"method"`
- <a id="s-a1debb9d63"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-dd87058e5b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources.md#src-5fd1cb5bbe) — [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bebc7e4eeb8947c0ccc37f5e54f021f299bb4a6f83699fc71a467ada22ed9c6d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "stove0-review-rclone-effect-target",
  "module": "stove0_review_rclone_effect_target",
  "name": "contract",
  "owner": "stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
