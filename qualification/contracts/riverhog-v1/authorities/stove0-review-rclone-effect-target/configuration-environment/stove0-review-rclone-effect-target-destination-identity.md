# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-destin-c984e537dc:a9e41190bb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3c35501075"></a>
| Field | Shape |
|---|---|
| <a id="s-31006fedc6"></a>`consumers` | ["stove0-review-rclone-effect-target"] |
| <a id="s-26574b8277"></a>`default_expressions` | ["''"] |
| <a id="s-e67ce255bb"></a>`id` | "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY" |
| <a id="s-8748b2ca37"></a>`input_shape` | "environment-string" |
| <a id="s-446e3477ab"></a>`name` | "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY" |
| <a id="s-f2a83e023e"></a>`owner` | "stove0-review-rclone-effect-target" |

## Governing policies

- <a id="pa-2bbaf85d0a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY](../../../evidence/sources.md#src-13f168e9ef) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_DESTINATION_IDENTITY', '')` |

### Machine authority

- `/external_contract/configuration_environment/209`

### Exact owned JSON

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
