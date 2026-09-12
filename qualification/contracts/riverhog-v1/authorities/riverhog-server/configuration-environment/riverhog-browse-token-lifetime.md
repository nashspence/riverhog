# RIVERHOG_BROWSE_TOKEN_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-browse-token-lifetime:f4c15db32c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ee975e99ad"></a>
| Field | Shape |
|---|---|
| <a id="s-cdaf9eecb6"></a>`classification` | "runtime" |
| <a id="s-ca55d6dc58"></a>`consumers` | ["riverhog-server"] |
| <a id="s-c5e5c6f3c4"></a>`disposition` | "contractual" |
| <a id="s-777167e457"></a>`id` | "riverhog-server:environment:RIVERHOG_BROWSE_TOKEN_LIFETIME" |
| <a id="s-1a8ef76883"></a>`name` | "RIVERHOG_BROWSE_TOKEN_LIFETIME" |
| <a id="s-1095e66092"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-400cbf425c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_BROWSE_TOKEN_LIFETIME](../../../evidence/sources.md#src-9cc099a19b) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_BROWSE_TOKEN_LIFETIME', '24h')` |

### Machine authority

- `/external_contract/configuration_environment/44`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ddba26eea26de4604a83e0fdeae698c185504156ef9d3c2e033a62f155666c2 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_BROWSE_TOKEN_LIFETIME",
  "name": "RIVERHOG_BROWSE_TOKEN_LIFETIME",
  "owner": "riverhog-server"
}
```
