# A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-image-id:d2b4dad022 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d13e9ae004"></a>

| Field | Value |
|---|---|
| <a id="s-f0e3fee02f"></a>`consumers` | `["a-stove0-nvenc-av1-opus-target"]` |
| <a id="s-e3d6152482"></a>`default_expressions` | `["''"]` |
| <a id="s-e0d8eea5f2"></a>`id` | `"a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_ID"` |
| <a id="s-4c3d51fadb"></a>`input_shape` | `"environment-string"` |
| <a id="s-2c255b04b2"></a>`name` | `"A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_ID"` |
| <a id="s-d26a81b88c"></a>`owner` | `"a-stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-4fb6b815ef"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-nvenc-av1-opus-target:A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_ID](../../../evidence/sources/authorities.md#src-65dc49daa5) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py::\_image\_id](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-nvenc-av1-opus-target` | [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py) | `os.getenv(f'{prefix}_IMAGE_ID', '')` |

### Machine authority

- `/external_contract/configuration_environment/136`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98b727c803052b59889b4727b6ceb4a692d1ec54dbb50a014700fc4391092d51 -->

```json
{
  "consumers": [
    "a-stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_ID",
  "input_shape": "environment-string",
  "name": "A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_ID",
  "owner": "a-stove0-nvenc-av1-opus-target"
}
```

</details>
