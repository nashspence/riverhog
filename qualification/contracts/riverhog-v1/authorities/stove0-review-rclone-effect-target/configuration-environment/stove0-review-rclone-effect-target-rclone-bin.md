# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-rclone-bin:3cdba72a78 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-986a5dc23a"></a>

| Field | Value |
|---|---|
| <a id="s-72c970d79e"></a>`consumers` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-d2217a5807"></a>`default_expressions` | `["'rclone'"]` |
| <a id="s-e7469eabe2"></a>`id` | `"stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_BIN"` |
| <a id="s-f0db4b7c89"></a>`input_shape` | `"environment-string"` |
| <a id="s-adf35c1f47"></a>`name` | `"STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_BIN"` |
| <a id="s-14d34e06dd"></a>`owner` | `"stove0-review-rclone-effect-target"` |

## Governing policies

- <a id="pa-ce116dee93"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_BIN](../../../evidence/sources/authorities.md#src-8731ef7028) — [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py::\_effect\_destination](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py) | `os.getenv(f'{PREFIX}_RCLONE_BIN', 'rclone')` |

### Machine authority

- `/external_contract/configuration_environment/213`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3463b863ebfc2a5c97b547cebf8d5e103894d4fc909bd9ac7543ed1fc4dadb4b -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "'rclone'"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_BIN",
  "owner": "stove0-review-rclone-effect-target"
}
```

</details>
