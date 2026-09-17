# STOVE0_REVIEW_MATERIALIZE_TARGET_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-workspace:097959581f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9978433062"></a>

| Field | Value |
|---|---|
| <a id="s-05ea931a8b"></a>`consumers` | `["stove0-review-materialize-target"]` |
| <a id="s-164afd85d0"></a>`default_expressions` | `["'/run/stove0-review'"]` |
| <a id="s-2d54433db7"></a>`id` | `"stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_WORKSPACE"` |
| <a id="s-d3338c8191"></a>`input_shape` | `"environment-string"` |
| <a id="s-0ebd5e82c4"></a>`name` | `"STOVE0_REVIEW_MATERIALIZE_TARGET_WORKSPACE"` |
| <a id="s-22cb055147"></a>`owner` | `"stove0-review-materialize-target"` |

## Governing policies

- <a id="pa-df01bed33c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_WORKSPACE](../../../evidence/sources/authorities.md#src-c812dca17e) — [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/app.py::main](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/app.py](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py) | `os.getenv(f'{PREFIX}_WORKSPACE', '/run/stove0-review')` |

### Machine authority

- `/external_contract/configuration_environment/208`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5982f01118cfaf587a8c57317c28815ed61aad2147c113f1e4dac0b2271d43f7 -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "'/run/stove0-review'"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_WORKSPACE",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_WORKSPACE",
  "owner": "stove0-review-materialize-target"
}
```

</details>
