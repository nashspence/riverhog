# STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-zstd:abf9c3be21 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-878b94fc4c"></a>

| Field | Value |
|---|---|
| <a id="s-5577bc1efe"></a>`consumers` | `["stove0-nvenc-av1-opus-target"]` |
| <a id="s-62d5424dac"></a>`default_expressions` | `["'zstd'"]` |
| <a id="s-74edd7d6cc"></a>`id` | `"stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD"` |
| <a id="s-2a05dd31cf"></a>`input_shape` | `"environment-string"` |
| <a id="s-6bcf4de7ec"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD"` |
| <a id="s-dcd2efd7de"></a>`owner` | `"stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-cf5594af6c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD](../../../evidence/sources.md#src-6bc691e964) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/source_artifacts.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/source_artifacts.py` | `os.environ.get('STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD', 'zstd')` |

### Machine authority

- `/external_contract/configuration_environment/181`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ccdcfc21270aba51346450a091ecb18565fdbaf05bc365026f3ca768886c4e43 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'zstd'"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD",
  "owner": "stove0-nvenc-av1-opus-target"
}
```

</details>
