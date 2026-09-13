# stove0_review_rclone_effect_target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target:cd70d3a6e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-cc9bfbb727) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-75e5eab418"></a>
| Field | Shape |
|---|---|
| <a id="s-0d84e8303f"></a>`candidate_id` | "python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target" |
| <a id="s-c1e604ba99"></a>`distribution` | "stove0-review-rclone-effect-target" |
| <a id="s-e8534e6024"></a>`exports` | additional keys=`RcloneReviewDestination`, `ReviewRcloneEffectTargetService` |
| <a id="s-de597bbf9c"></a>`module` | "stove0_review_rclone_effect_target" |

## Governing policies

- <a id="pa-7b47c7ac9a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../../../evidence/sources.md#src-5fd1cb5bbe) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/__init__.py`

### Machine authority

- `/external_contract/python/51`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92df12c40624f6e74f7150b9c8e0103401af41447e0adc70e04be8a359a9c081 -->

```json
{
  "candidate_id": "python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target",
  "distribution": "stove0-review-rclone-effect-target",
  "exports": {
    "RcloneReviewDestination": {
      "fields": [
        {
          "default": "required",
          "name": "identity",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "remote",
          "type": "'str'"
        },
        {
          "default": "None",
          "name": "config_path",
          "type": "'Path | None'"
        },
        {
          "default": "'rclone'",
          "name": "executable",
          "type": "'str'"
        },
        {
          "default": "86400",
          "name": "timeout_seconds",
          "type": "'int'"
        }
      ],
      "kind": "class",
      "members": {
        "commit": {
          "kind": "method",
          "signature": "\"(self, *, delivery_id: 'str', output_root: 'Path', artifacts: 'Sequence[OutputArtifact]', manifest_path: 'Path') -> 'dict[str, JsonValue]'\""
        }
      },
      "signature": "\"(identity: 'str', remote: 'str', config_path: 'Path | None' = None, executable: 'str' = 'rclone', timeout_seconds: 'int' = 86400) -> None\""
    },
    "ReviewRcloneEffectTargetService": {
      "kind": "class",
      "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', destination: 'RcloneReviewDestination', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
    }
  },
  "module": "stove0_review_rclone_effect_target"
}
```
