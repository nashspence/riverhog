# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-source-revision:dbbe0789a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-dfda7a7a4a"></a>

| Field | Value |
|---|---|
| <a id="s-47f3bdaf5e"></a>`consumers` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-e3b07c3ef7"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-81786082bc"></a>`id` | `"stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SOURCE_REVISION"` |
| <a id="s-a71ba1fcc4"></a>`input_shape` | `"environment-string"` |
| <a id="s-9d1fbfc618"></a>`name` | `"STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SOURCE_REVISION"` |
| <a id="s-9d55402f49"></a>`owner` | `"stove0-review-rclone-effect-target"` |

## Governing policies

- <a id="pa-fbd0f142eb"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SOURCE_REVISION](../../../evidence/sources/authorities.md#src-119536f200) — [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py::main](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py) | `os.getenv(f'{PREFIX}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/219`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23abac1e7173fb37d6838e99493d09486c3ef351c7caa0eeab94c77dc8bcd1c4 -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SOURCE_REVISION",
  "owner": "stove0-review-rclone-effect-target"
}
```

</details>
