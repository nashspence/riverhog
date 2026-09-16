# stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-review-e3f8237810:f997512e76 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-106936430a"></a>
- <a id="s-5e78e824aa"></a>`distribution`: `stove0-review-rclone-effect-target`
- <a id="s-e782285ff5"></a>`module`: `stove0_review_rclone_effect_target`
- <a id="s-593fbce19d"></a>`name`: `prune_terminal_state`
- <a id="s-e8584990df"></a>`owner`: `stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService`
- <a id="s-a434b43cfc"></a>`unit`: `member`

### Declared structure

- <a id="s-cfb9c8032f"></a>`kind`: `"method"`
- <a id="s-ebc07cc5f2"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-557b2f1eca"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources.md#src-5fd1cb5bbe) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c84e4bfc52067119381bc52e7bc266b695f635bba988c1586c31d69a56a80b2e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "stove0-review-rclone-effect-target",
  "module": "stove0_review_rclone_effect_target",
  "name": "prune_terminal_state",
  "owner": "stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
