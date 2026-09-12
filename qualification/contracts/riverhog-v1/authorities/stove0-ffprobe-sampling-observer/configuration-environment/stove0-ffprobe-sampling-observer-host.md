# STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-host:99c7652adc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-bdc14c36c2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b5b56da79c"></a>
| Field | Shape |
|---|---|
| <a id="s-d01640759b"></a>`classification` | "identity" |
| <a id="s-c4c426b9e4"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-c8a2b65267"></a>`disposition` | "contractual" |
| <a id="s-f366076fc7"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST" |
| <a id="s-953e2f4a91"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST" |
| <a id="s-ba60d64f0b"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-f88fd3b8ec"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST](../../../evidence/sources.md#src-1ede0d200b) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/21/names` |
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/94`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a98fe50047451420177a80ebbb73531f473c2ebe7f8e6e56bb68d7cfb0f0b80 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
