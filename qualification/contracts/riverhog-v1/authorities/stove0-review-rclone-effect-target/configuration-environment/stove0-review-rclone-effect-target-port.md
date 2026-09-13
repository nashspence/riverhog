# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-port:24b8ab04d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1f1837a6f4"></a>
| Field | Shape |
|---|---|
| <a id="s-f961103742"></a>`consumers` | ["stove0-review-rclone-effect-target"] |
| <a id="s-4894f75578"></a>`default_expressions` | ["'8080'"] |
| <a id="s-388e15bf3c"></a>`id` | "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_PORT" |
| <a id="s-b005281b63"></a>`input_shape` | "environment-string" |
| <a id="s-e20e3d9deb"></a>`name` | "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_PORT" |
| <a id="s-75bdcd9bc3"></a>`owner` | "stove0-review-rclone-effect-target" |

## Governing policies

- <a id="pa-9fcb26f881"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_PORT](../../../evidence/sources.md#src-333e7531f6) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/212`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a7c7707a899e817a7c97d90a7083d7e62ea7d8e50c3a074d178eeab510cbddb -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_PORT",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_PORT",
  "owner": "stove0-review-rclone-effect-target"
}
```
