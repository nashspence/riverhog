# STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-samplers-json:01aaa819aa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-decbf9314d"></a>
| Field | Shape |
|---|---|
| <a id="s-f540a20671"></a>`consumers` | ["stove0-review-materialize-target"] |
| <a id="s-da8e120990"></a>`default_expressions` | ["unset"] |
| <a id="s-ff75883761"></a>`id` | "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON" |
| <a id="s-40c15532e4"></a>`input_shape` | "environment-string" |
| <a id="s-933d21d395"></a>`name` | "STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON" |
| <a id="s-c02703316b"></a>`owner` | "stove0-review-materialize-target" |

## Governing policies

- <a id="pa-60794f7ed9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON](../../../evidence/sources.md#src-b822de9d0e) — `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | `reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py` | `os.getenv(f'{PREFIX}_SAMPLERS_JSON')` |

### Machine authority

- `/external_contract/configuration_environment/202`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f87ffc45a8a306d0f39add3ff3b36c892bb0c5118db304ace13894717db4b582 -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON",
  "owner": "stove0-review-materialize-target"
}
```
