# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffmpeg-bin:4d8a7bb05d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d15fb4b928"></a>
| Field | Shape |
|---|---|
| <a id="s-98b9d4f5c3"></a>`consumers` | ["stove0-nvenc-av1-opus-review-sampler","stove0-nvenc-av1-opus-target","stove0-opus-review-sampler","stove0-opus-target"] |
| <a id="s-a54012d8c0"></a>`name` | "STOVE0_FFMPEG_BIN" |

## Governing policies

- <a id="pa-c1678f2d09"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_FFMPEG_BIN](../../../evidence/sources.md#src-a352d58c15) — `configuration-environment:STOVE0_FFMPEG_BIN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/95`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 413f1a9293c253aeb8b41eb3dcc1501edcd3bc1265a39ef4b2571b643c7da447 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler",
    "stove0-nvenc-av1-opus-target",
    "stove0-opus-review-sampler",
    "stove0-opus-target"
  ],
  "name": "STOVE0_FFMPEG_BIN"
}
```
