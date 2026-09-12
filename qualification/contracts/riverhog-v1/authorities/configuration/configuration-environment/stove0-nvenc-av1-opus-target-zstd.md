# STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-nvenc-av1-opus-target-zstd:a633458eff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-62bc3f7cc9"></a>
| Field | Shape |
|---|---|
| <a id="s-119f21dafe"></a>`consumers` | ["stove0-nvenc-av1-opus-target"] |
| <a id="s-02fadb9af0"></a>`name` | "STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD" |

## Governing policies

- <a id="pa-b4f69ef996"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD](../../../evidence/sources.md#src-2745a75884) — `configuration-environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/106`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 06523f8282fa53b1c6d15af90137aa9ebaaf1cdfd9556c4c92a12d8f4c4928e0 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD"
}
```
