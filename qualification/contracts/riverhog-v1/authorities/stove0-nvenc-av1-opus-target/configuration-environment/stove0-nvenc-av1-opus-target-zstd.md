# STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-zstd:f137deda7b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-321a6bfb89) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9f9eafd5d5"></a>
| Field | Shape |
|---|---|
| <a id="s-d541fe1cd2"></a>`classification` | "identity" |
| <a id="s-8248147251"></a>`consumers` | ["stove0-nvenc-av1-opus-target"] |
| <a id="s-73c8b85867"></a>`disposition` | "contractual" |
| <a id="s-8211605975"></a>`id` | "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD" |
| <a id="s-0e24792ee3"></a>`name` | "STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD" |
| <a id="s-48214e7d5d"></a>`owner` | "stove0-nvenc-av1-opus-target" |

## Governing policies

- <a id="pa-9c1bd2b89c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD](../../../evidence/sources.md#src-6bc691e964) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/source_artifacts.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/23/names` |
| parser | `stove0-nvenc-av1-opus-target` | `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/source_artifacts.py` | `os.environ.get('STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD', 'zstd')` |

### Machine authority

- `/external_contract/configuration_environment/103`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30ece4fdafea359c917d92cf4302c57095a9685e70c683e1f77a6163c971503c -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "disposition": "contractual",
  "id": "stove0-nvenc-av1-opus-target:environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD",
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD",
  "owner": "stove0-nvenc-av1-opus-target"
}
```
