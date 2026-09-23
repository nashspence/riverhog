# A_STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-zstd:384a60f9c9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d0c45c1a2b"></a>

| Field | Value |
|---|---|
| <a id="s-47cc2d8760"></a>`consumers` | `["a-stove0-nvenc-av1-opus-target"]` |
| <a id="s-d2d1e00bbb"></a>`default_expressions` | `["'zstd'"]` |
| <a id="s-820d19aacb"></a>`id` | `"a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD"` |
| <a id="s-2953890d77"></a>`input_shape` | `"environment-string"` |
| <a id="s-a1035d4640"></a>`name` | `"A_STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD"` |
| <a id="s-5796450659"></a>`owner` | `"a-stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-3f3c24fce5"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-nvenc-av1-opus-target:A_STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD](../../../evidence/sources/authorities.md#src-603e715371) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/source\_artifacts.py::\_zstd\_command](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/source_artifacts.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-nvenc-av1-opus-target` | [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/source\_artifacts.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/source_artifacts.py) | `os.environ.get('A_STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD', 'zstd')` |

### Machine authority

- `/external_contract/configuration_environment/143`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34c609eff1519ef626536532cb6d461de8c62e2f3645e29f3949cec95381c6b6 -->

```json
{
  "consumers": [
    "a-stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'zstd'"
  ],
  "id": "a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD",
  "input_shape": "environment-string",
  "name": "A_STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD",
  "owner": "a-stove0-nvenc-av1-opus-target"
}
```

</details>
