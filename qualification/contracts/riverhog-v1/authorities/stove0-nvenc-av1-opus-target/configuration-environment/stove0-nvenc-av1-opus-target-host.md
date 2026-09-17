# STOVE0_NVENC_AV1_OPUS_TARGET_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-host:b5a721ce86 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5a6a419251"></a>

| Field | Value |
|---|---|
| <a id="s-d1adf7e576"></a>`consumers` | `["stove0-nvenc-av1-opus-target"]` |
| <a id="s-8b1df3c62a"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-484c2fd384"></a>`id` | `"stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_HOST"` |
| <a id="s-5a3e13b794"></a>`input_shape` | `"environment-string"` |
| <a id="s-893eec5e8f"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_TARGET_HOST"` |
| <a id="s-322eda2582"></a>`owner` | `"stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-4a08021003"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_HOST](../../../evidence/sources.md#src-3754100ddf) — [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/app.py::\_parser](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/app.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py) | `os.getenv(f'{prefix}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/173`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76ea6ea83284090afb927b5eaf3f8c1acebbc5f519872b38bf6225837c6e810e -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_HOST",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_HOST",
  "owner": "stove0-nvenc-av1-opus-target"
}
```

</details>
