# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-ffmpeg-bin:3a8619ef69 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-b0c1830930) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-aa4f15d37f"></a>
| Field | Shape |
|---|---|
| <a id="s-c2a9438d60"></a>`consumers` | ["stove0-opus-target"] |
| <a id="s-d1035c754d"></a>`default_expressions` | ["'ffmpeg'"] |
| <a id="s-0fc161ae60"></a>`id` | "stove0-opus-target:environment:STOVE0_FFMPEG_BIN" |
| <a id="s-cc2b365de6"></a>`input_shape` | "environment-string" |
| <a id="s-c35c4f35c0"></a>`name` | "STOVE0_FFMPEG_BIN" |
| <a id="s-213db7b774"></a>`owner` | "stove0-opus-target" |

## Governing policies

- <a id="pa-8610ddbf69"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-target:STOVE0_FFMPEG_BIN](../../../evidence/sources.md#src-854b16672a) — `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-target` | `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py` | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/190`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce90955c8e39640f61efdd927eed644792c28b9f3f3b0eefb880d93b96aaf9bf -->

```json
{
  "consumers": [
    "stove0-opus-target"
  ],
  "default_expressions": [
    "'ffmpeg'"
  ],
  "id": "stove0-opus-target:environment:STOVE0_FFMPEG_BIN",
  "input_shape": "environment-string",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "stove0-opus-target"
}
```
