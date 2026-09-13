# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-workspace:7895aba8d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c406d69a96"></a>
| Field | Shape |
|---|---|
| <a id="s-2c6a23ce32"></a>`consumers` | ["stove0-review-rclone-effect-target"] |
| <a id="s-7e17b4ea81"></a>`default_expressions` | ["'/run/stove0-review'"] |
| <a id="s-64dc759cad"></a>`id` | "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE" |
| <a id="s-29da3b1cb0"></a>`input_shape` | "environment-string" |
| <a id="s-5192164e70"></a>`name` | "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE" |
| <a id="s-1a0d87a02e"></a>`owner` | "stove0-review-rclone-effect-target" |

## Governing policies

- <a id="pa-f365938f28"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE](../../../evidence/sources.md#src-a92b6498b5) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_WORKSPACE', '/run/stove0-review')` |

### Machine authority

- `/external_contract/configuration_environment/223`

### Exact owned JSON

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
