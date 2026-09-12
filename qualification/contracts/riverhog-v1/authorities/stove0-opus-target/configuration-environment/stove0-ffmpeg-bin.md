# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-ffmpeg-bin:b3eafe930a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-396c2dee76) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5f56b9ebf5"></a>
| Field | Shape |
|---|---|
| <a id="s-2818dc622e"></a>`classification` | "identity" |
| <a id="s-451b5d705f"></a>`consumers` | ["stove0-opus-target"] |
| <a id="s-1da2da1273"></a>`disposition` | "contractual" |
| <a id="s-1b1b7d2c5f"></a>`id` | "stove0-opus-target:environment:STOVE0_FFMPEG_BIN" |
| <a id="s-7dbd023a7b"></a>`name` | "STOVE0_FFMPEG_BIN" |
| <a id="s-d0a5790f47"></a>`owner` | "stove0-opus-target" |

## Governing policies

- <a id="pa-3e36a07f33"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-opus-target:STOVE0_FFMPEG_BIN](../../../evidence/sources.md#src-854b16672a) — `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/25/names` |
| parser | `stove0-opus-target` | `reference/stove0/targets/opus/target/src/stove0_opus_target/app.py` | `os.getenv('STOVE0_FFMPEG_BIN', 'ffmpeg')` |

### Machine authority

- `/external_contract/configuration_environment/105`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 887040e4eada762b88551f85502496dfcf0dee1f9913ef2efe10809474b20466 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-opus-target"
  ],
  "disposition": "contractual",
  "id": "stove0-opus-target:environment:STOVE0_FFMPEG_BIN",
  "name": "STOVE0_FFMPEG_BIN",
  "owner": "stove0-opus-target"
}
```
