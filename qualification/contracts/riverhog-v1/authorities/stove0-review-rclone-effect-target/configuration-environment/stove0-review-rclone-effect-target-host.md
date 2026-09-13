# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-host:0c548b1193 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-574fe9b8e4"></a>
| Field | Shape |
|---|---|
| <a id="s-a7b2406b64"></a>`consumers` | ["stove0-review-rclone-effect-target"] |
| <a id="s-5396bdb526"></a>`default_expressions` | ["'127.0.0.1'"] |
| <a id="s-75451e7e9e"></a>`id` | "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_HOST" |
| <a id="s-47cc75791d"></a>`input_shape` | "environment-string" |
| <a id="s-600a648358"></a>`name` | "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_HOST" |
| <a id="s-16a516924a"></a>`owner` | "stove0-review-rclone-effect-target" |

## Governing policies

- <a id="pa-fed7d0aac3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_HOST](../../../evidence/sources.md#src-eec7e56995) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/210`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 67f898eb1736b9f322b4b3f2631773bdc751e00c1c356ef255adaaf0ce96a62a -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_HOST",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_HOST",
  "owner": "stove0-review-rclone-effect-target"
}
```
