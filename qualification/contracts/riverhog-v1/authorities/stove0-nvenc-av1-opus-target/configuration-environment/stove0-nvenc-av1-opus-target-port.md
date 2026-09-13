# STOVE0_NVENC_AV1_OPUS_TARGET_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-port:201c5f56de -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-bd4498fcba"></a>
| Field | Shape |
|---|---|
| <a id="s-b833b4fe07"></a>`consumers` | ["stove0-nvenc-av1-opus-target"] |
| <a id="s-66e522afce"></a>`default_expressions` | ["'8080'"] |
| <a id="s-5e1fc08ef8"></a>`id` | "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_PORT" |
| <a id="s-5b74b79655"></a>`input_shape` | "environment-string" |
| <a id="s-9a7cb5cb3c"></a>`name` | "STOVE0_NVENC_AV1_OPUS_TARGET_PORT" |
| <a id="s-7dd1a5ae30"></a>`owner` | "stove0-nvenc-av1-opus-target" |

## Governing policies

- <a id="pa-6e27c72acc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_PORT](../../../evidence/sources.md#src-fb16b17c23) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py` | `os.getenv(f'{prefix}_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/175`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17022405104c09823a29fe0da7f11c2ed0961a52c079e6cc9b882f9153e0fd3c -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_PORT",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_PORT",
  "owner": "stove0-nvenc-av1-opus-target"
}
```
