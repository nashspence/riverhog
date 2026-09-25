# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-nvenc-av1-opus-sampler:stove0-ffmpeg-bin:1d9b1dd3aa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-nvenc-av1-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-dc3b9e297a"></a>

| Field | Value |
|---|---|
| <a id="s-2cfcf2ca71"></a>`consumers` | `["a-review0-nvenc-av1-opus-sampler"]` |
| <a id="s-221130ca92"></a>`default_expressions` | `["'ffmpeg'"]` |
| <a id="s-57bcabee8d"></a>`id` | `"a-review0-nvenc-av1-opus-sampler:environment:STOVE0_FFMPEG_BIN"` |
| <a id="s-fd0988394a"></a>`input_shape` | `"environment-string"` |
| <a id="s-3d0c74b453"></a>`name` | `"STOVE0_FFMPEG_BIN"` |
| <a id="s-90ff047361"></a>`owner` | `"a-review0-nvenc-av1-opus-sampler"` |

## Governing policies

- <a id="pa-1018ed2020"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-nvenc-av1-opus-sampler:STOVE0_FFMPEG_BIN](../../../evidence/sources/authorities.md#src-245b9aba83) — [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py::main](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-nvenc-av1-opus-sampler` | [some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a\_review0\_nvenc\_av1\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/app.py) | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/19`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 848b47d0706b330ab36fbd9b175042e5dde4da5b0582740172dcedf09729be27 -->

```json
{
  "consumers": [
    "a-review0-nvenc-av1-opus-sampler"
  ],
  "default_expressions": [
    "'ffmpeg'"
  ],
  "id": "a-review0-nvenc-av1-opus-sampler:environment:STOVE0_FFMPEG_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "a-review0-nvenc-av1-opus-sampler"
}
```

</details>
