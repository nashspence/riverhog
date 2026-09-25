# A_STOVE0_OPUS_TARGET_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-opus-target:a-stove0-opus-target-token:30b7578f79 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b5b56da79c"></a>

| Field | Value |
|---|---|
| <a id="s-c4c426b9e4"></a>`consumers` | `["a-stove0-opus-target"]` |
| <a id="s-3371f237e2"></a>`default_expressions` | `["unset"]` |
| <a id="s-f366076fc7"></a>`id` | `"a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_TOKEN"` |
| <a id="s-8ecc30e0a1"></a>`input_shape` | `"environment-string"` |
| <a id="s-953e2f4a91"></a>`name` | `"A_STOVE0_OPUS_TARGET_TOKEN"` |
| <a id="s-ba60d64f0b"></a>`owner` | `"a-stove0-opus-target"` |

## Governing policies

- <a id="pa-840ba72a07"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-opus-target:A_STOVE0_OPUS_TARGET_TOKEN](../../../evidence/sources/authorities.md#src-9d0f28aaf0) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::\_secret](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py); [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::target\_main](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.getenv(f'{prefix}_TOKEN')` |
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.environ.pop(f'{prefix}_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/94`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf5976e1a3a9c317524a5bfc91f4df3c10b496093a69fdaaf31130a133e1f94d -->

```json
{
  "consumers": [
    "a-stove0-opus-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_TOKEN",
  "input_shape": "environment-string",
  "name": "A_STOVE0_OPUS_TARGET_TOKEN",
  "owner": "a-stove0-opus-target"
}
```

</details>
