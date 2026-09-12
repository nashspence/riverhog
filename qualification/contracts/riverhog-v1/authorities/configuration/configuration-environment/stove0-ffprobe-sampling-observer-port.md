# STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-port:1ca9562401 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-551254e2cbf6"></a>
| Field | Shape |
|---|---|
| <a id="s-27a1ad1a866b"></a>`consumers` | ["stove0-ffprobe-sampling-observer"] |
| <a id="s-fd2ae4a599da"></a>`name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT" |

## Governing policies

- <a id="pa-0f0d2e88cec4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT](../../../evidence/sources.md#src-40fb82896300) — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/99`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6cbe5720f297baaa86b2e0f7a8017f8b313ae51ff786e714107609b55cd0d4c -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT"
}
```
