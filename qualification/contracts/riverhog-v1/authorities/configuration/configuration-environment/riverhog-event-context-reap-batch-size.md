# RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-event-context-reap-batch-size:d85fcf5925 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-ee8d4c5bb25d"></a>
| Field | Shape |
|---|---|
| <a id="s-9cef198e9cb1"></a>`consumers` | ["riverhog-server"] |
| <a id="s-199cea8e22a7"></a>`name` | "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](#s-ee8d4c5bb25d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-62bbe8e979c2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-d565db79d8e0"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../../../evidence/sources.md#src-066eb69a6c18) — `configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/37`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: afd313c075766908be097411390f7e51b843be812fb38dbb6372f13164a15e97 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE"
}
```
