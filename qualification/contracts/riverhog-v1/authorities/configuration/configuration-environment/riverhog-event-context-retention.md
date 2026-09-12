# RIVERHOG_EVENT_CONTEXT_RETENTION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-event-context-retention:a9503b2042 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c6af80cc8d0e"></a>
| Field | Shape |
|---|---|
| <a id="s-dc771f99e615"></a>`consumers` | ["riverhog-server"] |
| <a id="s-3869830d71a6"></a>`name` | "RIVERHOG_EVENT_CONTEXT_RETENTION" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="RIVERHOG_EVENT_CONTEXT_RETENTION"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_EVENT_CONTEXT_RETENTION](#s-c6af80cc8d0e) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-22ac30a60171"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-13ed77b5de19"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_EVENT_CONTEXT_RETENTION](../../../evidence/sources.md#src-7bf791cfe76d) — `configuration-environment:RIVERHOG_EVENT_CONTEXT_RETENTION`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/38`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d2855fe397fd15ef6b0d0ebfb2ed8ba449685461210fc0deff7d42b2bcd9f536 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_EVENT_CONTEXT_RETENTION"
}
```
