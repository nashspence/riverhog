# STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-token-file:867145d0af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-1d31d466df) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-551254e2cb"></a>
| Field | Shape |
|---|---|
| <a id="s-e74d09636c"></a>`classification` | "credential" |
| <a id="s-27a1ad1a86"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-e1e626e479"></a>`disposition` | "contractual" |
| <a id="s-3a0a03e6eb"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE" |
| <a id="s-fd2ae4a599"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE" |
| <a id="s-50b4a1ec36"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-c592dd8068"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE](../../../evidence/sources.md#src-476999fdb4) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/20/names` |
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE')` |

### Machine authority

- `/external_contract/configuration_environment/99`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 470e5f3c31fedf1f6c98fd8b7b3651c553e2389a3683f934ea1bf056ddf1c7b8 -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
