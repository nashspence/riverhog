# STOVE0_REVIEW_RCLONE_EFFECT_TARGET_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target-image-digest:8f73e10b5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-rclone-effect-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ca48950b02"></a>

| Field | Value |
|---|---|
| <a id="s-7fa6d62383"></a>`consumers` | `["stove0-review-rclone-effect-target"]` |
| <a id="s-05645c32dd"></a>`default_expressions` | `["''"]` |
| <a id="s-95fbc5f58a"></a>`id` | `"stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_IMAGE_DIGEST"` |
| <a id="s-a43ef310a3"></a>`input_shape` | `"environment-string"` |
| <a id="s-ec1467afc9"></a>`name` | `"STOVE0_REVIEW_RCLONE_EFFECT_TARGET_IMAGE_DIGEST"` |
| <a id="s-bb162cee71"></a>`owner` | `"stove0-review-rclone-effect-target"` |

## Governing policies

- <a id="pa-2f948f304b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_IMAGE_DIGEST](../../../evidence/sources.md#src-b9a8b82806) — `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-rclone-effect-target` | `reference/stove0/targets/review/rclone-effect-target/src/stove0_review_rclone_effect_target/app.py` | `os.getenv(f'{PREFIX}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/211`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b45c00beb6801efc864d8be8ee61c80ab056248bb05de41473c3fdafb467d00b -->

```json
{
  "consumers": [
    "stove0-review-rclone-effect-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-review-rclone-effect-target:environment:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_RCLONE_EFFECT_TARGET_IMAGE_DIGEST",
  "owner": "stove0-review-rclone-effect-target"
}
```

</details>
