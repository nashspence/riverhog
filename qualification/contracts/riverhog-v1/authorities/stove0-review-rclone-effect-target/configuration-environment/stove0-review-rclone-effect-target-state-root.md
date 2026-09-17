# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_STATE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-state-root:d0df403a63 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b1ba755d84"></a>

| Field | Value |
|---|---|
| <a id="s-c26d089493"></a>`consumers` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-49bffd7273"></a>`default_expressions` | `["'/var/lib/stove0-review-rclone-effect-target'"]` |
| <a id="s-3789b05811"></a>`id` | `"stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_STATE_ROOT"` |
| <a id="s-085e8e61c2"></a>`input_shape` | `"environment-string"` |
| <a id="s-593baac063"></a>`name` | `"STOVE0_REVIEW_RCLONE_EFFECT_TARGET_STATE_ROOT"` |
| <a id="s-4486e5a845"></a>`owner` | `"stove0-review-rclone-effect-target"` |

## Governing policies

- <a id="pa-4674bd1f78"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_STATE_ROOT](../../../evidence/sources/authorities.md#src-34de43ae77) — [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py::main](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py) | `os.getenv(f'{PREFIX}_STATE_ROOT', '/var/lib/stove0-review-rclone-effect-target')` |

### Machine authority

- `/external_contract/configuration_environment/220`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f4f59e4bba8412e0ce87901eb70fe3e1d60a2c91a5e08464517375739a281cf -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "'/var/lib/stove0-review-rclone-effect-target'"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_STATE_ROOT",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_STATE_ROOT",
  "owner": "stove0-review-rclone-effect-target"
}
```

</details>
