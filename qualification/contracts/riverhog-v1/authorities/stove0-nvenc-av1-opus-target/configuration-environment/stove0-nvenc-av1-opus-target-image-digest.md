# STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-image-digest:3e78399085 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3ad467ed2b"></a>
| Field | Shape |
|---|---|
| <a id="s-fcf72def57"></a>`consumers` | ["stove0-nvenc-av1-opus-target"] |
| <a id="s-e5a282beb7"></a>`default_expressions` | ["''"] |
| <a id="s-addf591d9b"></a>`id` | "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_DIGEST" |
| <a id="s-d074d0c28a"></a>`input_shape` | "environment-string" |
| <a id="s-ebb9ec7233"></a>`name` | "STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_DIGEST" |
| <a id="s-ab6d564bd6"></a>`owner` | "stove0-nvenc-av1-opus-target" |

## Governing policies

- <a id="pa-86d2a59942"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_DIGEST](../../../evidence/sources.md#src-fd772770ec) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py` | `os.getenv(f'{prefix}_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/174`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42d5fb9e8ca2c5b596478a1e925a3b4b7a9e0366fc4c523879b10fdb60527b4e -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_DIGEST",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_DIGEST",
  "owner": "stove0-nvenc-av1-opus-target"
}
```
