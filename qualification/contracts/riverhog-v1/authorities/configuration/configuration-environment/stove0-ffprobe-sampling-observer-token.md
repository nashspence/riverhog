# STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-token:c9cf9e6b2e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-eb8f28adc2"></a>
| Field | Shape |
|---|---|
| <a id="s-f44dec2d22"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-c875192905"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN" |

## Governing policies

- <a id="pa-0666775fc2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN](../../../evidence/sources.md#src-6184c716d4) — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/101`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0d251d0a2d60388477f04c9d7df9af9230c5071ea6adb13a1ae023002772adc -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN"
}
```
