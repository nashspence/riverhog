# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-ffmpeg-bin:85a13ab220 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e0718e3ae1"></a>
| Field | Shape |
|---|---|
| <a id="s-53c473dfe1"></a>`consumers` | ["stove0-nvenc-av1-opus-target"] |
| <a id="s-f1ca622372"></a>`default_expressions` | ["'ffmpeg'"] |
| <a id="s-b157606d60"></a>`id` | "stove0-nvenc-av1-opus-target:environment:STOVE0_FFMPEG_BIN" |
| <a id="s-2e49c887d8"></a>`input_shape` | "environment-string" |
| <a id="s-d3243722bc"></a>`name` | "STOVE0_FFMPEG_BIN" |
| <a id="s-ff4993ce76"></a>`owner` | "stove0-nvenc-av1-opus-target" |

## Governing policies

- <a id="pa-699923f96a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_FFMPEG_BIN](../../../evidence/sources.md#src-648abac914) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py` | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/172`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2771f370ba87dd17070874e54be7e52f223212908f2446d403f93b018eced9da -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'ffmpeg'"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_FFMPEG_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "stove0-nvenc-av1-opus-target"
}
```
