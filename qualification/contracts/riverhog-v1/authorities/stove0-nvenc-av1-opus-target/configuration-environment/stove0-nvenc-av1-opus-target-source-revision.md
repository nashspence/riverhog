# STOVE0_NVENC_AV1_OPUS_TARGET_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-source-revision:d7e5331350 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-dac516bc47"></a>

| Field | Value |
|---|---|
| <a id="s-457ed73ac2"></a>`consumers` | `["stove0-nvenc-av1-opus-target"]` |
| <a id="s-63608cfcca"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-26cf215710"></a>`id` | `"stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_SOURCE_REVISION"` |
| <a id="s-c508465db1"></a>`input_shape` | `"environment-string"` |
| <a id="s-9c80ef951b"></a>`name` | `"STOVE0_NVENC_AV1_OPUS_TARGET_SOURCE_REVISION"` |
| <a id="s-946b8d2b86"></a>`owner` | `"stove0-nvenc-av1-opus-target"` |

## Governing policies

- <a id="pa-cba64e2ad9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_SOURCE_REVISION](../../../evidence/sources.md#src-5e5b556d66) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py` | `os.getenv(f'{prefix}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/176`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2094d7fcff006b6bce843840d5a930e0ce1a57e73d678ae998368607c3b4a1b9 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_SOURCE_REVISION",
  "owner": "stove0-nvenc-av1-opus-target"
}
```

</details>
