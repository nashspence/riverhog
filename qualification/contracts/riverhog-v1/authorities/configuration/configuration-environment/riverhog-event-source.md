# RIVERHOG_EVENT_SOURCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-event-source:c7769bbb88 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-71ed3f559b"></a>
| Field | Shape |
|---|---|
| <a id="s-93ede9eab6"></a>`consumers` | ["riverhog-server"] |
| <a id="s-a3d7c530e4"></a>`name` | "RIVERHOG_EVENT_SOURCE" |

## Governing policies

- <a id="pa-e2508e35ef"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_EVENT_SOURCE](../../../evidence/sources.md#src-6c9accca57) — `configuration-environment:RIVERHOG_EVENT_SOURCE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/39`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 064ef6535473b031ad1e473acc663f9f48dd3ebc75e1c9ae3bba89fd19945756 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_EVENT_SOURCE"
}
```
