# STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-port:52fb4e8e3d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-bdc14c36c2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6e5c2a5e00"></a>
| Field | Shape |
|---|---|
| <a id="s-bd628ef749"></a>`classification` | "identity" |
| <a id="s-8adc3e5c22"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-26788ec61e"></a>`disposition` | "contractual" |
| <a id="s-61aa35b3fe"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT" |
| <a id="s-bc8e0830d2"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT" |
| <a id="s-8e15fbe367"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-5e4d59a197"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT](../../../evidence/sources.md#src-b6e1a506b4) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/21/names` |
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/96`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e77fcb65ee8e93f1413541aeeff4e74379fc28eb2bd9173489bf810a204190b4 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
