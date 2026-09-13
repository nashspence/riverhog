# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-token-file:b1ae9c72f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-d9132f9888) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-15035a28e0"></a>
| Field | Shape |
|---|---|
| <a id="s-950ec387ff"></a>`consumers` | ["stove0-review-rclone-effect-target"] |
| <a id="s-ad12dc509a"></a>`default_expressions` | ["unset"] |
| <a id="s-85a7db9bee"></a>`id` | "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN_FILE" |
| <a id="s-48f3fe11a2"></a>`input_shape` | "environment-string" |
| <a id="s-20ead7c234"></a>`name` | "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN_FILE" |
| <a id="s-b5a7f86294"></a>`owner` | "stove0-review-rclone-effect-target" |

## Governing policies

- <a id="pa-671e134340"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN_FILE](../../../evidence/sources.md#src-58167f74c0) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/222`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4ac071425af2c981bb3b55fbf2026cad34d1645fc1a034e56264f6b80b0fce7 -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN_FILE",
  "owner": "stove0-review-rclone-effect-target"
}
```
