# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-rclone-a2dc4f3a37:d3e26dda03 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-637c0687c8"></a>
| Field | Shape |
|---|---|
| <a id="s-d510a34fe7"></a>`consumers` | ["stove0-review-rclone-effect-target"] |
| <a id="s-bcd1eb8d6b"></a>`default_expressions` | ["'86400'"] |
| <a id="s-094063ee21"></a>`id` | "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS" |
| <a id="s-c99cd0a627"></a>`input_shape` | "environment-string" |
| <a id="s-91f311aaa8"></a>`name` | "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS" |
| <a id="s-0a7e0dfdeb"></a>`owner` | "stove0-review-rclone-effect-target" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS"; consumers=["stove0-review-rclone-effect-target"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS](#s-637c0687c8) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-9e5cbf7c11"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-981dec5ebe"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS](../../../evidence/sources.md#src-7f004c5323) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_RCLONE_TIMEOUT_SECONDS', '86400')` |

### Machine authority

- `/external_contract/configuration_environment/216`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cfedae544b5b61271f44dad0b3bf43f61a314a1bfdc0c78d09c7743c98624f94 -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "'86400'"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS",
  "owner": "stove0-review-rclone-effect-target"
}
```
