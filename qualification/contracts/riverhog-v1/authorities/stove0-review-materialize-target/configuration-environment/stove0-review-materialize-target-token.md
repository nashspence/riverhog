# STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-token:b9bb1b5c48 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-474ca4f8b4) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e165d7abb5"></a>
| Field | Shape |
|---|---|
| <a id="s-768abe4cba"></a>`consumers` | ["stove0-review-materialize-target"] |
| <a id="s-c54f8b3b56"></a>`default_expressions` | ["unset"] |
| <a id="s-cd815b3b93"></a>`id` | "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN" |
| <a id="s-17612734d0"></a>`input_shape` | "environment-string" |
| <a id="s-c9adfbff2f"></a>`name` | "STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN" |
| <a id="s-8cacd73240"></a>`owner` | "stove0-review-materialize-target" |

## Governing policies

- <a id="pa-df8d1effec"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN](../../../evidence/sources.md#src-d8db24208d) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py` | `os.getenv(f'{PREFIX}_TOKEN')` |
| parser | `stove0-review-materialize-target` | `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py` | `os.environ.pop(f'{PREFIX}_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/206`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e844ded3238539c7e6351db952579321ccd157d4da181d86e7aa6aca98cdc69 -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN",
  "owner": "stove0-review-materialize-target"
}
```
