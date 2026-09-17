# STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-samplers-json-file:c051d4161d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-13620c3fea"></a>

| Field | Value |
|---|---|
| <a id="s-11f3d08040"></a>`consumers` | `["stove0-review-materialize-target"]` |
| <a id="s-9d822879a1"></a>`default_expressions` | `["unset"]` |
| <a id="s-90a83ea7d9"></a>`id` | `"stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON_FILE"` |
| <a id="s-2c31df2fca"></a>`input_shape` | `"environment-string"` |
| <a id="s-d88606930a"></a>`name` | `"STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON_FILE"` |
| <a id="s-e21f91e942"></a>`owner` | `"stove0-review-materialize-target"` |

## Governing policies

- <a id="pa-42014c1275"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON_FILE](../../../evidence/sources/authorities.md#src-0f35eb76bc) — [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/app.py::\_sampler\_registrations](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/app.py](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py) | `os.getenv(f'{PREFIX}_SAMPLERS_JSON_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/203`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44f3dd7ef7d93bac85e9f79dfff1373a83748b6ef1323c9d140cd357a69bf808 -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON_FILE",
  "owner": "stove0-review-materialize-target"
}
```

</details>
