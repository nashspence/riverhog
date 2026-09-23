# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-review0-opus-sampler:stove0-ffmpeg-bin:578ecf4dac -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-opus-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2d908ff8df"></a>

| Field | Value |
|---|---|
| <a id="s-c799298a70"></a>`consumers` | `["a-review0-opus-sampler"]` |
| <a id="s-539ad8b835"></a>`default_expressions` | `["'ffmpeg'"]` |
| <a id="s-69dffd9a14"></a>`id` | `"a-review0-opus-sampler:environment:STOVE0_FFMPEG_BIN"` |
| <a id="s-f8a11ba5cc"></a>`input_shape` | `"environment-string"` |
| <a id="s-c530fa2f45"></a>`name` | `"STOVE0_FFMPEG_BIN"` |
| <a id="s-c6f518f261"></a>`owner` | `"a-review0-opus-sampler"` |

## Governing policies

- <a id="pa-75e319a243"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-review0-opus-sampler:STOVE0_FFMPEG_BIN](../../../evidence/sources/authorities.md#src-47bcc9e1c1) — [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py::main](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-review0-opus-sampler` | [some-implementations/stove0/review0/samplers/opus/src/a\_review0\_opus\_sampler/app.py](../../../../../../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/app.py) | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/30`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d398ea01f060c7c74951e9e497cc97301daf7532ca6a5e06982b9d79f09df028 -->

```json
{
  "consumers": [
    "a-review0-opus-sampler"
  ],
  "default_expressions": [
    "'ffmpeg'"
  ],
  "id": "a-review0-opus-sampler:environment:STOVE0_FFMPEG_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "a-review0-opus-sampler"
}
```

</details>
