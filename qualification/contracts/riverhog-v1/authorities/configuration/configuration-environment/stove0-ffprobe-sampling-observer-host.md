# STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-host:953d8d15ee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5b5ad305a732"></a>
| Field | Shape |
|---|---|
| <a id="s-d3dd57dbb2b2"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-cf768fb7d869"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST" |

## Governing policies

- <a id="pa-e3592c52d77f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST](../../../evidence/sources.md#src-e883355c399d) — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/97`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 809719e9cc2c52ef8ae6d5226b96fbeeaccd269bd261f12154475b8affc525d6 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST"
}
```
