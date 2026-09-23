# A_STOVE0_NVENC_AV1_OPUS_TARGET_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-port:d530570c75 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-da1cc2577c"></a>

| Field | Value |
|---|---|
| <a id="s-1a4b8e932d"></a>`consumers` | `["a-stove0-nvenc-av1-opus-target"]` |
| <a id="s-3b3bfc86f1"></a>`default_expressions` | `["'8080'"]` |
| <a id="s-68e3e7cef2"></a>`id` | `"a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_PORT"` |
| <a id="s-30e76985b7"></a>`input_shape` | `"environment-string"` |
| <a id="s-2d5445bb11"></a>`name` | `"A_STOVE0_NVENC_AV1_OPUS_TARGET_PORT"` |
| <a id="s-86be1d53d0"></a>`owner` | `"a-stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-8b4e58189f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-nvenc-av1-opus-target:A_STOVE0_NVENC_AV1_OPUS_TARGET_PORT](../../../evidence/sources/authorities.md#src-34fb9c522f) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py::\_parser](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-nvenc-av1-opus-target` | [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py) | `os.getenv(f'{prefix}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/137`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5be78ba41df045eb5c188a40c814c433b6a16ec405a1281f87ae612dadf93dd9 -->

```json
{
  "consumers": [
    "a-stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_PORT",
  "input_shape": "environment-string",
  "name": "A_STOVE0_NVENC_AV1_OPUS_TARGET_PORT",
  "owner": "a-stove0-nvenc-av1-opus-target"
}
```

</details>
