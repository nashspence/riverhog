# A_STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-state-root:9e0a6baee2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-da273161b4"></a>

| Field | Value |
|---|---|
| <a id="s-1e7b15fb47"></a>`consumers` | `["a-stove0-nvenc-av1-opus-target"]` |
| <a id="s-71838a4472"></a>`default_expressions` | `["'/var/lib/a-stove0-nvenc-av1-opus-target'"]` |
| <a id="s-b1614ba9ed"></a>`id` | `"a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT"` |
| <a id="s-55e127dee3"></a>`input_shape` | `"environment-string"` |
| <a id="s-8fab4aadb8"></a>`name` | `"A_STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT"` |
| <a id="s-bc9e95e985"></a>`owner` | `"a-stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-efc88c67f2"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-nvenc-av1-opus-target:A_STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT](../../../evidence/sources/authorities.md#src-403ad688e3) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py::target\_main](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-nvenc-av1-opus-target` | [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py) | `os.getenv(f'{prefix}_STATE_ROOT', '/var/lib/a-stove0-nvenc-av1-opus-target')` |

### Machine authority

- `/external_contract/configuration_environment/139`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcd2550d50aa47dc6f6073ec97a6f260c6b918fd52331b5506ba7b322915f896 -->

```json
{
  "consumers": [
    "a-stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'/var/lib/a-stove0-nvenc-av1-opus-target'"
  ],
  "id": "a-stove0-nvenc-av1-opus-target:environment:A_STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT",
  "input_shape": "environment-string",
  "name": "A_STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT",
  "owner": "a-stove0-nvenc-av1-opus-target"
}
```

</details>
