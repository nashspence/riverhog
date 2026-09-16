# stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-review-056d169a45:b2642307cd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a8a7f9fa0d"></a>
- <a id="s-444acb13b5"></a>`distribution`: `stove0-review-rclone-effect-target`
- <a id="s-62e746a86b"></a>`module`: `stove0_review_rclone_effect_target`
- <a id="s-f26262e0e8"></a>`name`: `ReviewRcloneEffectTargetService`
- <a id="s-321dfd6b1e"></a>`unit`: `export`

### Declared structure

- <a id="s-9ed50a7219"></a>`kind`: `"class"`
- <a id="s-714f4e9a06"></a>`signature`: `"\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', destination: 'RcloneReviewDestination', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [put_job](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice-put-job.md)
- [contract](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice-contract.md)
- [close](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice-close.md)
- [cancel_job](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice-cancel-job.md)
- [get_job](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice-get-job.md)
- [preflight](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice-preflight.md)
- [readiness](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice-readiness.md)
- [prune_terminal_state](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice-prune-terminal-state.md)

## Governing policies

- <a id="pa-cd66221ae1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources.md#src-5fd1cb5bbe) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84045a676dd088a31542ed3f3988a0af5223dc7a4b3eef08b16def9cb080b1de -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', destination: 'RcloneReviewDestination', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "stove0-review-rclone-effect-target",
  "module": "stove0_review_rclone_effect_target",
  "name": "ReviewRcloneEffectTargetService",
  "unit": "export"
}
```

</details>
