# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-workspace:7895aba8d1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c406d69a96"></a>

| Field | Value |
|---|---|
| <a id="s-2c6a23ce32"></a>`consumers` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-7e17b4ea81"></a>`default_expressions` | `["'/run/stove0-review'"]` |
| <a id="s-64dc759cad"></a>`id` | `"stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE"` |
| <a id="s-29da3b1cb0"></a>`input_shape` | `"environment-string"` |
| <a id="s-5192164e70"></a>`name` | `"STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE"` |
| <a id="s-1a0d87a02e"></a>`owner` | `"stove0-review-rclone-effect-target"` |

## Governing policies

- <a id="pa-f365938f28"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE](../../../evidence/sources/authorities.md#src-a92b6498b5) — [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py::main](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py) | `os.getenv(f'{PREFIX}_WORKSPACE', '/run/stove0-review')` |

### Machine authority

- `/external_contract/configuration_environment/223`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f369f0564c6f6d480f836e9a642422d5d761e15dc89288334c0c48a465997f0 -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "'/run/stove0-review'"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE",
  "owner": "stove0-review-rclone-effect-target"
}
```

</details>
