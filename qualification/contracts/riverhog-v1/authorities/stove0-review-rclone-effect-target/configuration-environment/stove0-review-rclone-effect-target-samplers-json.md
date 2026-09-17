# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-samplers-json:d198567ce2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2a3f124dad"></a>

| Field | Value |
|---|---|
| <a id="s-4b39157865"></a>`consumers` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-97b263b2b9"></a>`default_expressions` | `["unset"]` |
| <a id="s-c556f2f26b"></a>`id` | `"stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON"` |
| <a id="s-577a558dba"></a>`input_shape` | `"environment-string"` |
| <a id="s-5a42b163c0"></a>`name` | `"STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON"` |
| <a id="s-c60ee59f23"></a>`owner` | `"stove0-review-rclone-effect-target"` |

## Governing policies

- <a id="pa-a561f1f879"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON](../../../evidence/sources/authorities.md#src-442d08ddb7) — [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py::\_sampler\_registrations](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py) | `os.getenv(f'{PREFIX}_SAMPLERS_JSON')` |

### Machine authority

- `/external_contract/configuration_environment/217`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 739ba392c73fcc4ca6631790cd3074963861931aaac155226f5e99841e7cdd21 -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON",
  "owner": "stove0-review-rclone-effect-target"
}
```

</details>
