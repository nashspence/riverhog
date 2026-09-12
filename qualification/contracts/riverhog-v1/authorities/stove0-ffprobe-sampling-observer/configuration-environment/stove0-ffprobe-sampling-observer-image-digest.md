# STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-image-digest:fa63f5e3fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-bdc14c36c2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d15fb4b928"></a>
| Field | Shape |
|---|---|
| <a id="s-11bde062b6"></a>`classification` | "identity" |
| <a id="s-98b9d4f5c3"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-bb4ce41eed"></a>`disposition` | "contractual" |
| <a id="s-7c79245e59"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST" |
| <a id="s-a54012d8c0"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST" |
| <a id="s-fe903fc9c1"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-89d56cbe98"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST](../../../evidence/sources.md#src-64a7dc58ec) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/21/names` |
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST', '')` |

### Machine authority

- `/external_contract/configuration_environment/95`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73bd5c433695c06d7a2b2210bd9f11dd801131a6a75b3535e975b637a629f5be -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
