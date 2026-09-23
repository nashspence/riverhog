# A_STOVE0_OPUS_TARGET_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-opus-target:a-stove0-opus-target-source-revision:502f6419ba -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d6e9a2d362"></a>

| Field | Value |
|---|---|
| <a id="s-3f756a949d"></a>`consumers` | `["a-stove0-opus-target"]` |
| <a id="s-519e87b83b"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-28d94b8700"></a>`id` | `"a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_SOURCE_REVISION"` |
| <a id="s-4777e8907c"></a>`input_shape` | `"environment-string"` |
| <a id="s-cbc0ee4f3e"></a>`name` | `"A_STOVE0_OPUS_TARGET_SOURCE_REVISION"` |
| <a id="s-70282cfd79"></a>`owner` | `"a-stove0-opus-target"` |

## Governing policies

- <a id="pa-0c07889aed"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-opus-target:A_STOVE0_OPUS_TARGET_SOURCE_REVISION](../../../evidence/sources/authorities.md#src-0c6d8c7ff6) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::target\_main](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.getenv(f'{prefix}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/148`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ad0fbd12de4b66c13c725bf52c172155b84e54db84acf8c891eb9e815585f07 -->

```json
{
  "consumers": [
    "a-stove0-opus-target"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "A_STOVE0_OPUS_TARGET_SOURCE_REVISION",
  "owner": "a-stove0-opus-target"
}
```

</details>
