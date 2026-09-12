# STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-source-revision:bb124ca481 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-74f1ce0aa1"></a>
| Field | Shape |
|---|---|
| <a id="s-14c637bf7d"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-9911339263"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION" |

## Governing policies

- <a id="pa-8e90363a90"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION](../../../evidence/sources.md#src-eb5066e041) — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/91`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c92e6da496ce8e0a6a293a3a9cc6e480544c6abd981136be3dc3a2420a15443 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION"
}
```
