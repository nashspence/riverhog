# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_REMOTE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-rclone-remote:8a5d919a61 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c82cab358d"></a>

| Field | Value |
|---|---|
| <a id="s-f0100a0ac6"></a>`consumers` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-581879113c"></a>`default_expressions` | `["''"]` |
| <a id="s-b36f3fa42b"></a>`id` | `"stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_REMOTE"` |
| <a id="s-09a7e478c8"></a>`input_shape` | `"environment-string"` |
| <a id="s-bbb490c3c3"></a>`name` | `"STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_REMOTE"` |
| <a id="s-f14596ccbe"></a>`owner` | `"stove0-review-rclone-effect-target"` |

## Governing policies

- <a id="pa-9dab589c0d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_REMOTE](../../../evidence/sources.md#src-c6d8df82f9) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_RCLONE_REMOTE', '')` |

### Machine authority

- `/external_contract/configuration_environment/215`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c332fa2be105de234378cecc010077f72fef7910f2295352f927c87b0c2ceb4 -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_REMOTE",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_REMOTE",
  "owner": "stove0-review-rclone-effect-target"
}
```

</details>
