# stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-review-d36c49c5a7:2fcf6e2a5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c2cd148680"></a>
- <a id="s-4ace807133"></a>`distribution`: `stove0-review-rclone-effect-target`
- <a id="s-d715c179a1"></a>`module`: `stove0_review_rclone_effect_target`
- <a id="s-1d39e0690b"></a>`name`: `preflight`
- <a id="s-c42e849414"></a>`owner`: `stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService`
- <a id="s-521ee07b7d"></a>`unit`: `member`

### Declared structure

- <a id="s-17356b83fc"></a>`kind`: `"method"`
- <a id="s-89d9fedbd0"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [ReviewRcloneEffectTargetService](stove0-review-rclone-effect-target-reviewrcloneeffecttargetservice.md)

## Governing policies

- <a id="pa-eb19fa4d27"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources.md#src-5fd1cb5bbe) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b8e1ee7a18451e8602a61caae9760f0bd7f58da924b7152cf6599515b443a68 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-review-rclone-effect-target",
  "module": "stove0_review_rclone_effect_target",
  "name": "preflight",
  "owner": "stove0_review_rclone_effect_target.ReviewRcloneEffectTargetService",
  "unit": "member"
}
```

</details>
