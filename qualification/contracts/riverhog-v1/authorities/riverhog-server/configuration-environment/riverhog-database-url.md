# RIVERHOG_DATABASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-database-url:f8616f5e45 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](families/credential/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ef146f919d"></a>
| Field | Shape |
|---|---|
| <a id="s-1160270360"></a>`classification` | "credential" |
| <a id="s-93a23b729e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-3bee20e781"></a>`disposition` | "contractual" |
| <a id="s-7df70bf7e9"></a>`id` | "riverhog-server:environment:RIVERHOG_DATABASE_URL" |
| <a id="s-4e7ddbc9fe"></a>`name` | "RIVERHOG_DATABASE_URL" |
| <a id="s-3e6453e97a"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-3c8c25f483"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_DATABASE_URL](../../../evidence/sources.md#src-c84b874748) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/12/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_DATABASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/52`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6f35b71f9970987fa500395d2955a2b4364e48df81fc3b611f4c43bd1cbd084 -->

```json
{
  "classification": "credential",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_DATABASE_URL",
  "name": "RIVERHOG_DATABASE_URL",
  "owner": "riverhog-server"
}
```
