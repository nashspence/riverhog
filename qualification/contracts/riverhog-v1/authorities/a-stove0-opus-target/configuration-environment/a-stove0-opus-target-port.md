# A_STOVE0_OPUS_TARGET_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-opus-target:a-stove0-opus-target-port:26a5117d03 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-74f1ce0aa1"></a>

| Field | Value |
|---|---|
| <a id="s-14c637bf7d"></a>`consumers` | `["a-stove0-opus-target"]` |
| <a id="s-438a310e71"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-5670701bbc"></a>`id` | `"a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_PORT"` |
| <a id="s-5218f9ba8a"></a>`input_shape` | `"environment-string"` |
| <a id="s-9911339263"></a>`name` | `"A_STOVE0_OPUS_TARGET_PORT"` |
| <a id="s-a4a31c47b8"></a>`owner` | `"a-stove0-opus-target"` |

## Governing policies

- <a id="pa-71bce18bb8"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-opus-target:A_STOVE0_OPUS_TARGET_PORT](../../../evidence/sources/authorities.md#src-1eaa40df83) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::\_parser](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.getenv(f'{prefix}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/91`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95fe8a3b1b502d5bebd933a9f995634c9ec2ec1404756f5c910a4443aa146610 -->

```json
{
  "consumers": [
    "a-stove0-opus-target"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_PORT",
  "input_shape": "environment-string",
  "name": "A_STOVE0_OPUS_TARGET_PORT",
  "owner": "a-stove0-opus-target"
}
```

</details>
