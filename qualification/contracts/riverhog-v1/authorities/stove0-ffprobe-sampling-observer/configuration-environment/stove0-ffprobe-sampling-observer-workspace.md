# STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-workspace:543444aa3e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-bdc14c36c2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-960cd64e68"></a>
| Field | Shape |
|---|---|
| <a id="s-61e7d2c41f"></a>`classification` | "identity" |
| <a id="s-194d4c0b99"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-a6c17ec2d9"></a>`disposition` | "contractual" |
| <a id="s-9b65d2f1fb"></a>`id` | "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE" |
| <a id="s-fc06ab9bf9"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE" |
| <a id="s-edda95a7cd"></a>`owner` | "stove0-ffprobe-sampling-observer" |

## Governing policies

- <a id="pa-678d62ea14"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE](../../../evidence/sources.md#src-c36f19fb69) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/21/names` |
| parser | `stove0-ffprobe-sampling-observer` | `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/app.py` | `os.getenv('STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE', '/run/stove0-ffprobe-sampling-observer')` |

### Machine authority

- `/external_contract/configuration_environment/100`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0888eaf6e3170266275d7bd675a0e30bcdccc7ec5ca8d34f7dfaa89130111760 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "disposition": "contractual",
  "id": "stove0-ffprobe-sampling-observer:environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE",
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE",
  "owner": "stove0-ffprobe-sampling-observer"
}
```
