# STOVE0_FFPROBE_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-bin:aa041d0328 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-bdc14c36c2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1513c088ba"></a>
| Field | Shape |
|---|---|
| <a id="s-e6c253fcab"></a>`classification` | "identity" |
| <a id="s-9d88b5d281"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-cd1668325e"></a>`disposition` | "contractual" |
| <a id="s-eeebfb92ac"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_BIN" |
| <a id="s-66c149272c"></a>`name` | "STOVE0_FFPROBE_BIN" |
| <a id="s-f9c1542d89"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-005b72ab9a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_BIN](../../../evidence/sources.md#src-2e8c92027a) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/21/names` |
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_BIN', 'ffprobe')` |

### Machine authority

- `/external_contract/configuration_environment/93`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e11e580a6199a9b28922361151ab196126d41ac008d6b619323668830e98ed5e -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_BIN",
  "name": "STOVE0_FFPROBE_BIN",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
