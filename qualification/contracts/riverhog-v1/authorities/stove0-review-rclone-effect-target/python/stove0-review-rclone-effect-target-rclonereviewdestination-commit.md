# stove0_review_rclone_effect_target.RcloneReviewDestination.commit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-rclone-1a19b9c2a7:01f2b26c61 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b65d6ca4cb"></a>
- <a id="s-d298a02325"></a>`distribution`: `stove0-review-rclone-effect-target`
- <a id="s-9eb7fc3cfd"></a>`module`: `stove0_review_rclone_effect_target`
- <a id="s-9c307cf45c"></a>`name`: `commit`
- <a id="s-cbc354c4bd"></a>`owner`: `stove0_review_rclone_effect_target.RcloneReviewDestination`
- <a id="s-f8d73c27fb"></a>`unit`: `member`

### Declared structure

- <a id="s-ed51a5aec3"></a>`kind`: `"method"`
- <a id="s-ca5a1b1f58"></a>`signature`: `"\"(self, *, delivery_id: 'str', output_root: 'Path', artifacts: 'Sequence[OutputArtifact]', manifest_path: 'Path') -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [RcloneReviewDestination](stove0-review-rclone-effect-target-rclonereviewdestination.md)

## Governing policies

- <a id="pa-fc71120196"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources.md#src-5fd1cb5bbe) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_rclone_effect_target.RcloneReviewDestination.commit`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 125f63550e0a78107d79dcd5b323320a05d7e00984343dc88f4eb7b0c0cd2f46 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, delivery_id: 'str', output_root: 'Path', artifacts: 'Sequence[OutputArtifact]', manifest_path: 'Path') -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-review-rclone-effect-target",
  "module": "stove0_review_rclone_effect_target",
  "name": "commit",
  "owner": "stove0_review_rclone_effect_target.RcloneReviewDestination",
  "unit": "member"
}
```

</details>
