# STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-token-file:b91bab9f76 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-d8d2b81c4a) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-012f57c395"></a>
| Field | Shape |
|---|---|
| <a id="s-f63cd10e51"></a>`consumers` | ["stove0-nvenc-av1-opus-target"] |
| <a id="s-f05c1b04db"></a>`default_expressions` | ["unset"] |
| <a id="s-1315637fe6"></a>`id` | "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE" |
| <a id="s-cffe87d5d5"></a>`input_shape` | "environment-string" |
| <a id="s-888c85c5fc"></a>`name` | "STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE" |
| <a id="s-211166e278"></a>`owner` | "stove0-nvenc-av1-opus-target" |

## Governing policies

- <a id="pa-6c8267893a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE](../../../evidence/sources.md#src-a8872fc126) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-nvenc-av1-opus-target` | `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py` | `os.getenv(f'{prefix}_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/179`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a0ae28253bdbc1be57039a1dc276b743793d4fcb07b9751403be6c42e4e23e4 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE",
  "owner": "stove0-nvenc-av1-opus-target"
}
```
