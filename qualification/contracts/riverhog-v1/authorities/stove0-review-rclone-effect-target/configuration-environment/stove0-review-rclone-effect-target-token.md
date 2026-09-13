# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-token:845d3bcb8f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d379f95cd5"></a>
| Field | Shape |
|---|---|
| <a id="s-870e27d6d7"></a>`consumers` | ["stove0-review-rclone-effect-target"] |
| <a id="s-a6086f3882"></a>`default_expressions` | ["unset"] |
| <a id="s-77c851083b"></a>`id` | "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN" |
| <a id="s-f84395a381"></a>`input_shape` | "environment-string" |
| <a id="s-6d3338e47b"></a>`name` | "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN" |
| <a id="s-94769e9eea"></a>`owner` | "stove0-review-rclone-effect-target" |

## Governing policies

- <a id="pa-1ee42e7092"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN](../../../evidence/sources.md#src-f997a24e0d) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_TOKEN')` |
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.environ.pop(f'{PREFIX}_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/221`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e236a8a88aa8b4c047891df703d5061ae1a426737d958eb0d59681bbe60135d -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN",
  "owner": "stove0-review-rclone-effect-target"
}
```
