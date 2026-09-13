# STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-workspace:8eb41fac15 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-907a932813"></a>
| Field | Shape |
|---|---|
| <a id="s-73530a56ad"></a>`consumers` | ["stove0-nvenc-av1-opus-target"] |
| <a id="s-2a94cf11df"></a>`default_expressions` | ["'/run/stove0-nvenc-av1-opus-target'"] |
| <a id="s-1faee2967f"></a>`id` | "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE" |
| <a id="s-7a49144b06"></a>`input_shape` | "environment-string" |
| <a id="s-635f76daa3"></a>`name` | "STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE" |
| <a id="s-cbe2af7830"></a>`owner` | "stove0-nvenc-av1-opus-target" |

## Governing policies

- <a id="pa-64c678d7c0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE](../../../evidence/sources.md#src-08eeb13134) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py` | `os.getenv(f'{prefix}_WORKSPACE', '/run/stove0-nvenc-av1-opus-target')` |

### Machine authority

- `/external_contract/configuration_environment/180`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 94c6828ccc73603925054a50a268a461cf94ed264817187b38ab041024ebbbd2 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'/run/stove0-nvenc-av1-opus-target'"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE",
  "owner": "stove0-nvenc-av1-opus-target"
}
```
