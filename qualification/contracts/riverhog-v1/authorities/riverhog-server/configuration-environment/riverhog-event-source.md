# RIVERHOG_EVENT_SOURCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-event-source:6706643589 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](families/identity/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-021597da78"></a>
| Field | Shape |
|---|---|
| <a id="s-fb44eedbb2"></a>`classification` | "identity" |
| <a id="s-de450a9408"></a>`consumers` | ["riverhog-server"] |
| <a id="s-26fda41a31"></a>`disposition` | "contractual" |
| <a id="s-7b17b012fe"></a>`id` | "riverhog-server:environment:RIVERHOG_EVENT_SOURCE" |
| <a id="s-9d016ce2fa"></a>`name` | "RIVERHOG_EVENT_SOURCE" |
| <a id="s-1cfe26331e"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-cda3852358"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_EVENT_SOURCE](../../../evidence/sources.md#src-cc7e1cd9d7) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/13/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_EVENT_SOURCE', 'urn:riverhog')` |

### Machine authority

- `/external_contract/configuration_environment/55`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf90257f50d6a8a62d75c821435f2760da579c14c8b9f513aa9455837ee9d6d8 -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_EVENT_SOURCE",
  "name": "RIVERHOG_EVENT_SOURCE",
  "owner": "riverhog-server"
}
```
