# STOVE0_REVIEW_MATERIALIZE_TARGET_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-image-digest:0a2b0d7583 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b3dd8d4481"></a>
| Field | Shape |
|---|---|
| <a id="s-a5828c1b01"></a>`consumers` | ["stove0-review-materialize-target"] |
| <a id="s-24c34751f1"></a>`default_expressions` | ["''"] |
| <a id="s-7eb7d89410"></a>`id` | "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_IMAGE_DIGEST" |
| <a id="s-1dc905d619"></a>`input_shape` | "environment-string" |
| <a id="s-6a00ac0a51"></a>`name` | "STOVE0_REVIEW_MATERIALIZE_TARGET_IMAGE_DIGEST" |
| <a id="s-c913657c03"></a>`owner` | "stove0-review-materialize-target" |

## Governing policies

- <a id="pa-80df5741e7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_IMAGE_DIGEST](../../../evidence/sources.md#src-13191bb470) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py` | `os.getenv(f'{PREFIX}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/200`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 786096b00854ff697b0e212b49ab2ef42bf07e4c517301a7a0a3ffb58ca34893 -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_IMAGE_DIGEST",
  "owner": "stove0-review-materialize-target"
}
```
