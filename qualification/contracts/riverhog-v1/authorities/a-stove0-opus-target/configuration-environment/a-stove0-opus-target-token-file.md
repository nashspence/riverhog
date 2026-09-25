# A_STOVE0_OPUS_TARGET_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-opus-target:a-stove0-opus-target-token-file:0cdd3e58e2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d15fb4b928"></a>

| Field | Value |
|---|---|
| <a id="s-98b9d4f5c3"></a>`consumers` | `["a-stove0-opus-target"]` |
| <a id="s-a6c332bf33"></a>`default_expressions` | `["unset"]` |
| <a id="s-7c79245e59"></a>`id` | `"a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_TOKEN_FILE"` |
| <a id="s-32b8dc3225"></a>`input_shape` | `"environment-string"` |
| <a id="s-a54012d8c0"></a>`name` | `"A_STOVE0_OPUS_TARGET_TOKEN_FILE"` |
| <a id="s-fe903fc9c1"></a>`owner` | `"a-stove0-opus-target"` |

## Governing policies

- <a id="pa-d507e4a5dc"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-opus-target:A_STOVE0_OPUS_TARGET_TOKEN_FILE](../../../evidence/sources/authorities.md#src-a69cb5872d) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::\_secret](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.getenv(f'{prefix}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/95`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2dd0bf52cd053e948df93679db59d3765b338f4e1efc47083781bf1d9c5398b6 -->

```json
{
  "consumers": [
    "a-stove0-opus-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "A_STOVE0_OPUS_TARGET_TOKEN_FILE",
  "owner": "a-stove0-opus-target"
}
```

</details>
