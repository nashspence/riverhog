# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-opus-target:stove0-ffmpeg-bin:c784ec40d8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5b5ad305a7"></a>

| Field | Value |
|---|---|
| <a id="s-d3dd57dbb2"></a>`consumers` | `["a-stove0-opus-target"]` |
| <a id="s-c889318b3c"></a>`default_expressions` | `["'ffmpeg'"]` |
| <a id="s-6178250fdc"></a>`id` | `"a-stove0-opus-target:environment:STOVE0_FFMPEG_BIN"` |
| <a id="s-fe5e59c7f3"></a>`input_shape` | `"environment-string"` |
| <a id="s-cf768fb7d8"></a>`name` | `"STOVE0_FFMPEG_BIN"` |
| <a id="s-87339d9e18"></a>`owner` | `"a-stove0-opus-target"` |

## Governing policies

- <a id="pa-d7d77b7d73"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-opus-target:STOVE0_FFMPEG_BIN](../../../evidence/sources/authorities.md#src-b4bcae9571) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::target\_main](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/97`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08d807b0850fdadb1a086cf7b9efa2afacf317710ddf5935d32180e446a31aa8 -->

```json
{
  "consumers": [
    "a-stove0-opus-target"
  ],
  "default_expressions": [
    "'ffmpeg'"
  ],
  "id": "a-stove0-opus-target:environment:STOVE0_FFMPEG_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "a-stove0-opus-target"
}
```

</details>
