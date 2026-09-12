# RIVERHOG_ARCHIVE_WRITE_STORE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-write-store:021cf599e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](families/identity/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ba8bf00da0"></a>
| Field | Shape |
|---|---|
| <a id="s-0293d1e5da"></a>`classification` | "identity" |
| <a id="s-fefed5a14e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-51cc44215e"></a>`disposition` | "contractual" |
| <a id="s-6e9078eeef"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_WRITE_STORE" |
| <a id="s-5c224fe460"></a>`name` | "RIVERHOG_ARCHIVE_WRITE_STORE" |
| <a id="s-316a058fc1"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-0d02d2e3f2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_STORE](../../../evidence/sources.md#src-2a3a35733e) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/13/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `values.get('RIVERHOG_ARCHIVE_WRITE_STORE', names[0])` |

### Machine authority

- `/external_contract/configuration_environment/42`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 36a7323fb22df4964d47616f85683660c44614add50550a07ddfe52509dec40f -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_WRITE_STORE",
  "name": "RIVERHOG_ARCHIVE_WRITE_STORE",
  "owner": "riverhog-server"
}
```
