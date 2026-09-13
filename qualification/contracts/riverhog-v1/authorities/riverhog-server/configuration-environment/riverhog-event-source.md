# RIVERHOG_EVENT_SOURCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-event-source:0efd21f707 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-51bdf3ea73"></a>
| Field | Shape |
|---|---|
| <a id="s-46d6397232"></a>`consumers` | ["riverhog-server"] |
| <a id="s-c720b22b45"></a>`default_expressions` | ["'urn:riverhog'"] |
| <a id="s-8708489666"></a>`id` | "riverhog-server:environment:RIVERHOG_EVENT_SOURCE" |
| <a id="s-0bb8695d4e"></a>`input_shape` | "environment-string" |
| <a id="s-cb037aed18"></a>`name` | "RIVERHOG_EVENT_SOURCE" |
| <a id="s-869feebb7c"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-9dd967d966"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_EVENT_SOURCE](../../../evidence/sources.md#src-cc7e1cd9d7) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_EVENT_SOURCE', 'urn:riverhog')` |

### Machine authority

- `/external_contract/configuration_environment/60`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09c8643350d173b66a3e672f6cc1efbfacde6afdca3c3521c7d84934ae53373b -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'urn:riverhog'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_EVENT_SOURCE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_EVENT_SOURCE",
  "owner": "riverhog-server"
}
```
