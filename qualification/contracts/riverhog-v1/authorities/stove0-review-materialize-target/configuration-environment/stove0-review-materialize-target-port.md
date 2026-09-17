# STOVE0_REVIEW_MATERIALIZE_TARGET_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-port:09200d4e61 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e73f26dd50"></a>

| Field | Value |
|---|---|
| <a id="s-aca441222c"></a>`consumers` | `["stove0-review-materialize-target"]` |
| <a id="s-ac107dc046"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-268e9e6049"></a>`id` | `"stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_PORT"` |
| <a id="s-4630333763"></a>`input_shape` | `"environment-string"` |
| <a id="s-f95cc39852"></a>`name` | `"STOVE0_REVIEW_MATERIALIZE_TARGET_PORT"` |
| <a id="s-b3f721f522"></a>`owner` | `"stove0-review-materialize-target"` |

## Governing policies

- <a id="pa-8b6f46aa12"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_PORT](../../../evidence/sources/authorities.md#src-bafe419002) — [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/app.py::\_parser](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/app.py](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py) | `os.getenv(f'{PREFIX}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/201`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 169485902747f18c1221d3885e497f4352cb6190156e248c49046c99d7f103ad -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_PORT",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_PORT",
  "owner": "stove0-review-materialize-target"
}
```

</details>
