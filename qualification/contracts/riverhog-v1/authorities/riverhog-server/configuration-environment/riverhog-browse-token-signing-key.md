# RIVERHOG_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-browse-token-signing-key:8cd336d06c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](families/credential/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b86dc65d81"></a>
| Field | Shape |
|---|---|
| <a id="s-7a6e63525b"></a>`classification` | "credential" |
| <a id="s-1e81b89850"></a>`consumers` | ["riverhog-server"] |
| <a id="s-adf479974a"></a>`disposition` | "contractual" |
| <a id="s-657f4a2ce7"></a>`id` | "riverhog-server:environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY" |
| <a id="s-08fd5560b5"></a>`name` | "RIVERHOG_BROWSE_TOKEN_SIGNING_KEY" |
| <a id="s-6f4b6e09b4"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-e3368c4976"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY](../../../evidence/sources.md#src-70d116fdbc) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/12/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_BROWSE_TOKEN_SIGNING_KEY', '')` |

### Machine authority

- `/external_contract/configuration_environment/45`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 01a1c204ae607afd6581e39f3c8248e0484e45253308d81668f9658037d08ac9 -->

```json
{
  "classification": "credential",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY",
  "name": "RIVERHOG_BROWSE_TOKEN_SIGNING_KEY",
  "owner": "riverhog-server"
}
```
