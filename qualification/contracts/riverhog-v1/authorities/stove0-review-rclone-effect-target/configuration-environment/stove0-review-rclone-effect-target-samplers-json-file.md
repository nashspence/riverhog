# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-sample-2adf51430a:0711fd3fb9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-fd7d73e69a"></a>

| Field | Value |
|---|---|
| <a id="s-a55a59faa3"></a>`consumers` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-d1b08aa8b5"></a>`default_expressions` | `["unset"]` |
| <a id="s-46c2e22331"></a>`id` | `"stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON_FILE"` |
| <a id="s-cf58e17e41"></a>`input_shape` | `"environment-string"` |
| <a id="s-2148cfff12"></a>`name` | `"STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON_FILE"` |
| <a id="s-fe294b0ec7"></a>`owner` | `"stove0-review-rclone-effect-target"` |

## Governing policies

- <a id="pa-d29a8baa1f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON_FILE](../../../evidence/sources.md#src-f24df93f95) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_SAMPLERS_JSON_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/218`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7acf72d783d9ba218bd0ac578db4c61530edecff7f62a51bbc1e5c6433020c8c -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON_FILE",
  "owner": "stove0-review-rclone-effect-target"
}
```

</details>
