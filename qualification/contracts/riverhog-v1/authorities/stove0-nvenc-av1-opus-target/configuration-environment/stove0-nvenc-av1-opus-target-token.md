# STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-token:97a2b8b169 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-25e3b58251"></a>

| Field | Value |
|---|---|
| <a id="s-71ff5053cc"></a>`consumers` | `["stove0-nvenc-av1-opus-target"]` |
| <a id="s-161ddad828"></a>`default_expressions` | `["unset"]` |
| <a id="s-c822a85dc1"></a>`id` | `"stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN"` |
| <a id="s-2cab81cbef"></a>`input_shape` | `"environment-string"` |
| <a id="s-3b82021d67"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN"` |
| <a id="s-0b55c318c0"></a>`owner` | `"stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-a6af779bfe"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN](../../../evidence/sources.md#src-1b1ad33ef7) — [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/app.py::\_secret](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py); [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/app.py::target\_main](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/app.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py) | `os.getenv(f'{prefix}_TOKEN')` |
| parser | `stove0-nvenc-av1-opus-target` | [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/app.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py) | `os.environ.pop(f'{prefix}_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/178`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a91632ba3ce9fcb8da9717adcb76b168e15d80d3e52fba57c00a7a3667d17795 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN",
  "owner": "stove0-nvenc-av1-opus-target"
}
```

</details>
