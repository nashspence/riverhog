# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_CONFIG_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-rclone-a761792ad0:c69124d0e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-d9132f9888) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-99c8eb1c7d"></a>
| Field | Shape |
|---|---|
| <a id="s-a52c09abae"></a>`consumers` | ["stove0-review-rclone-effect-target"] |
| <a id="s-a10389ce2d"></a>`default_expressions` | ["''"] |
| <a id="s-62331dcb68"></a>`id` | "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_CONFIG_FILE" |
| <a id="s-662e6a0e21"></a>`input_shape` | "environment-string" |
| <a id="s-9e12e97dee"></a>`name` | "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_CONFIG_FILE" |
| <a id="s-abdde598fa"></a>`owner` | "stove0-review-rclone-effect-target" |

## Governing policies

- <a id="pa-1ecf9643b2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_CONFIG_FILE](../../../evidence/sources.md#src-ff3d8661eb) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_RCLONE_CONFIG_FILE', '')` |

### Machine authority

- `/external_contract/configuration_environment/214`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3cbc8b193720e210d8ca3ffd36e4e321f3294ac49426938dd31976080e66f0b1 -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_CONFIG_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_CONFIG_FILE",
  "owner": "stove0-review-rclone-effect-target"
}
```
