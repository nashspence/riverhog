# STOVE0_REVIEW_MATERIALIZE_TARGET_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-source-revision:239599b607 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-474ca4f8b4) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-eb8aec9d18"></a>
| Field | Shape |
|---|---|
| <a id="s-0a87e6a26d"></a>`consumers` | ["stove0-review-materialize-target"] |
| <a id="s-e548f6ba22"></a>`default_expressions` | ["'unknown'"] |
| <a id="s-919bb7b7ae"></a>`id` | "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_SOURCE_REVISION" |
| <a id="s-ef9ddf40ef"></a>`input_shape` | "environment-string" |
| <a id="s-1288e3cab6"></a>`name` | "STOVE0_REVIEW_MATERIALIZE_TARGET_SOURCE_REVISION" |
| <a id="s-5aacd24817"></a>`owner` | "stove0-review-materialize-target" |

## Governing policies

- <a id="pa-e90e5c5845"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_SOURCE_REVISION](../../../evidence/sources.md#src-3727f03fe5) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py` | `os.getenv(f'{PREFIX}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/204`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75e0c58948d955314f6ef9ad076690b29e6b7069d44533f564e041d731922184 -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_SOURCE_REVISION",
  "owner": "stove0-review-materialize-target"
}
```
