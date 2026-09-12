# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-ffmpeg-bin:69354c726d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-321a6bfb89) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-663ea2a8c9"></a>
| Field | Shape |
|---|---|
| <a id="s-39706c39b2"></a>`classification` | "identity" |
| <a id="s-e37a73aa7a"></a>`consumers` | ["stove0-nvenc-av1-opus-target"] |
| <a id="s-4a5d9899c5"></a>`disposition` | "contractual" |
| <a id="s-9b8c32cdd8"></a>`id` | "stove0-nvenc-av1-opus-target:environment:STOVE0_FFMPEG_BIN" |
| <a id="s-f850425dcc"></a>`name` | "STOVE0_FFMPEG_BIN" |
| <a id="s-79f4d1452a"></a>`owner` | "stove0-nvenc-av1-opus-target" |

## Governing policies

- <a id="pa-aa5ac9337f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_FFMPEG_BIN](../../../evidence/sources.md#src-648abac914) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/23/names` |
| parser | `stove0-nvenc-av1-opus-target` | `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/app.py` | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/102`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c00bcf3b54680c4e9d30f36c48147f054752c1fc63513464b8b228cbc5fea62 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "disposition": "contractual",
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_FFMPEG_BIN",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "stove0-nvenc-av1-opus-target"
}
```
