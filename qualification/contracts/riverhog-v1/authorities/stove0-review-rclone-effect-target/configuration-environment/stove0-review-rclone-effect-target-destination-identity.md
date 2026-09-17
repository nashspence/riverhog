# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-destin-c984e537dc:a9e41190bb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3c35501075"></a>

| Field | Value |
|---|---|
| <a id="s-31006fedc6"></a>`consumers` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-26574b8277"></a>`default_expressions` | `["''"]` |
| <a id="s-e67ce255bb"></a>`id` | `"stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY"` |
| <a id="s-8748b2ca37"></a>`input_shape` | `"environment-string"` |
| <a id="s-446e3477ab"></a>`name` | `"STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY"` |
| <a id="s-f2a83e023e"></a>`owner` | `"stove0-review-rclone-effect-target"` |

## Governing policies

- <a id="pa-2bbaf85d0a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY](../../../evidence/sources/authorities.md#src-13f168e9ef) — [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py::\_effect\_destination](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | [reference/stove0/targets/review/rclone-effect-target/src/stove0\_review\_rclone\_effect\_target/app.py](../../../../../../reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py) | `os.getenv(f'{PREFIX}_DESTINATION_IDENTITY', '')` |

### Machine authority

- `/external_contract/configuration_environment/209`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c16d332da731068549d33bce58690d466b2773df86738457611664571f459716 -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY",
  "owner": "stove0-review-rclone-effect-target"
}
```

</details>
