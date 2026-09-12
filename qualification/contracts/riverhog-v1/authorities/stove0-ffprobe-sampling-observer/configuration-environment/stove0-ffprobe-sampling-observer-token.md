# STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-token:7710a074f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-1d31d466df) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b3dd205ff4"></a>
| Field | Shape |
|---|---|
| <a id="s-31eac6c9b7"></a>`classification` | "credential" |
| <a id="s-fdfa36834a"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-af7adabe00"></a>`disposition` | "contractual" |
| <a id="s-b929a15be2"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN" |
| <a id="s-7a20528c0b"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN" |
| <a id="s-211ee68554"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-0138dcf3da"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN](../../../evidence/sources.md#src-42afc1d184) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/20/names` |
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.environ.pop('STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN')` |
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/98`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ab77e645d60de20d093c528e16c6f25fbcd9896af87b16029b487f9b004f8739 -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
