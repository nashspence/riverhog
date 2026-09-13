# STOVE0_REVIEW_MATERIALIZE_TARGET_STATE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-state-root:0b5dcc808e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-474ca4f8b4) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-db3079284d"></a>
| Field | Shape |
|---|---|
| <a id="s-29a59a341d"></a>`consumers` | ["stove0-review-materialize-target"] |
| <a id="s-4c06c96747"></a>`default_expressions` | ["'/var/lib/stove0-review-materialize-target'"] |
| <a id="s-f5e3342d1e"></a>`id` | "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_STATE_ROOT" |
| <a id="s-b760c5b8bf"></a>`input_shape` | "environment-string" |
| <a id="s-6b7538ff8b"></a>`name` | "STOVE0_REVIEW_MATERIALIZE_TARGET_STATE_ROOT" |
| <a id="s-601f416108"></a>`owner` | "stove0-review-materialize-target" |

## Governing policies

- <a id="pa-605309a4fb"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_STATE_ROOT](../../../evidence/sources.md#src-e6c90cc875) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py` | `os.getenv(f'{PREFIX}_STATE_ROOT', '/var/lib/stove0-review-materialize-target')` |

### Machine authority

- `/external_contract/configuration_environment/205`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d504ba27160dab641f5cdc0e05ec6c2a24dd1c2e2b693b79d5806d931570b2a -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "'/var/lib/stove0-review-materialize-target'"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_STATE_ROOT",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_STATE_ROOT",
  "owner": "stove0-review-materialize-target"
}
```
