# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-review-sampler:stove0-ffmpeg-bin:c9bf9f48df -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4ce7abb670"></a>

| Field | Value |
|---|---|
| <a id="s-ebe8a893a2"></a>`consumers` | `["stove0-opus-review-sampler"]` |
| <a id="s-003bc4201d"></a>`default_expressions` | `["'ffmpeg'"]` |
| <a id="s-f10ba9f7a8"></a>`id` | `"stove0-opus-review-sampler:environment:STOVE0_FFMPEG_BIN"` |
| <a id="s-7aa187d92b"></a>`input_shape` | `"environment-string"` |
| <a id="s-51849146a8"></a>`name` | `"STOVE0_FFMPEG_BIN"` |
| <a id="s-a7a04652dd"></a>`owner` | `"stove0-opus-review-sampler"` |

## Governing policies

- <a id="pa-3f01425c7d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-review-sampler:STOVE0_FFMPEG_BIN](../../../evidence/sources/authorities.md#src-194d028459) — [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py::main](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-review-sampler` | [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py) | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/182`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a5d16877097175d08dcfeadb8edbf4ce121fb371a32932e05ac68c354cf88a6 -->

```json
{
  "consumers": [
    "stove0-opus-review-sampler"
  ],
  "default_expressions": [
    "'ffmpeg'"
  ],
  "id": "stove0-opus-review-sampler:environment:STOVE0_FFMPEG_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "stove0-opus-review-sampler"
}
```

</details>
