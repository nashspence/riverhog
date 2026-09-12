# STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-source-revision:09ae0ab110 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-bdc14c36c2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5b5ad305a7"></a>
| Field | Shape |
|---|---|
| <a id="s-92a3ec4d64"></a>`classification` | "identity" |
| <a id="s-d3dd57dbb2"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-f979494633"></a>`disposition` | "contractual" |
| <a id="s-6178250fdc"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION" |
| <a id="s-cf768fb7d8"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION" |
| <a id="s-87339d9e18"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-da77fd89f5"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION](../../../evidence/sources.md#src-776ca532af) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/21/names` |
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/97`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d33687cfe1f3e4bd1d1d4e71febae46fb43362e393cae58cd7b84f6fdb38f50 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
