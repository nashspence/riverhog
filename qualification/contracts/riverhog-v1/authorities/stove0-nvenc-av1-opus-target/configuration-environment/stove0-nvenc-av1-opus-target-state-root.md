# STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-state-root:22f986efd7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-387607e502"></a>

| Field | Value |
|---|---|
| <a id="s-dd9424e6bc"></a>`consumers` | `["stove0-nvenc-av1-opus-target"]` |
| <a id="s-43cf01e1d2"></a>`default_expressions` | `["'/var/lib/stove0-nvenc-av1-opus-target'"]` |
| <a id="s-84fa5e52b6"></a>`id` | `"stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT"` |
| <a id="s-1939accc0e"></a>`input_shape` | `"environment-string"` |
| <a id="s-bd5b86f3b7"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT"` |
| <a id="s-c1e07431f9"></a>`owner` | `"stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-c0f15d3818"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT](../../../evidence/sources/authorities.md#src-d24f94160a) — [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/app.py::target\_main](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/app.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py) | `os.getenv(f'{prefix}_STATE_ROOT', '/var/lib/stove0-nvenc-av1-opus-target')` |

### Machine authority

- `/external_contract/configuration_environment/177`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8dba89205d40148f85ee822b9abedf25c47aa41279d279e8058e5c5a48849f72 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'/var/lib/stove0-nvenc-av1-opus-target'"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT",
  "owner": "stove0-nvenc-av1-opus-target"
}
```

</details>
