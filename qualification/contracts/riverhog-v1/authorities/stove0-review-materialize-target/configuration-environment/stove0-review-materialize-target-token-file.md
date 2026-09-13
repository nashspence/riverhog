# STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-token-file:f0d82004e1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-54e88eafea"></a>
| Field | Shape |
|---|---|
| <a id="s-e2fa106b85"></a>`consumers` | ["stove0-review-materialize-target"] |
| <a id="s-9643b1a43c"></a>`default_expressions` | ["unset"] |
| <a id="s-1259db6e32"></a>`id` | "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN_FILE" |
| <a id="s-594d422a64"></a>`input_shape` | "environment-string" |
| <a id="s-c9d635c0a6"></a>`name` | "STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN_FILE" |
| <a id="s-2c1c2f32be"></a>`owner` | "stove0-review-materialize-target" |

## Governing policies

- <a id="pa-8ee831f557"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN_FILE](../../../evidence/sources.md#src-569110a206) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py` | `os.getenv(f'{PREFIX}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/207`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6eefbf9d9b16da0bcbe42a46413b4c3285c1886e41fa5277b41d07cbdbd16090 -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN_FILE",
  "owner": "stove0-review-materialize-target"
}
```
