# A_STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-workspace:8a82d70874 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-284bbb4914"></a>

| Field | Value |
|---|---|
| <a id="s-c9aa0936f8"></a>`consumers` | `["a-stove0-nvenc-av1-opus-target"]` |
| <a id="s-bb482cec50"></a>`default_expressions` | `["'/run/a-stove0-nvenc-av1-opus-target'"]` |
| <a id="s-f91da4b941"></a>`id` | `"a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE"` |
| <a id="s-f863ec0947"></a>`input_shape` | `"environment-string"` |
| <a id="s-9f2399bb48"></a>`name` | `"A_STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE"` |
| <a id="s-1932bf0a08"></a>`owner` | `"a-stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-b698a7b965"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-nvenc-av1-opus-target:A_STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE](../../../evidence/sources/authorities.md#src-8c6778cd8e) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py::target\_main](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-nvenc-av1-opus-target` | [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py) | `os.getenv(f'{prefix}_WORKSPACE', '/run/a-stove0-nvenc-av1-opus-target')` |

### Machine authority

- `/external_contract/configuration_environment/86`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbad41885c7fc35db9b20ef555afd20be6b62417eea3d489d5f8cf02b2c7dadc -->

```json
{
  "consumers": [
    "a-stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'/run/a-stove0-nvenc-av1-opus-target'"
  ],
  "id": "a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE",
  "input_shape": "environment-string",
  "name": "A_STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE",
  "owner": "a-stove0-nvenc-av1-opus-target"
}
```

</details>
