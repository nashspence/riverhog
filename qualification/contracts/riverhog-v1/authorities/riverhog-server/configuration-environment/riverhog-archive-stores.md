# RIVERHOG_ARCHIVE_STORES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-stores:b860faf9ad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](families/identity/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c6af80cc8d"></a>
| Field | Shape |
|---|---|
| <a id="s-14baecd833"></a>`classification` | "identity" |
| <a id="s-dc771f99e6"></a>`consumers` | ["riverhog-server"] |
| <a id="s-c3a53620c8"></a>`disposition` | "contractual" |
| <a id="s-a725fad84c"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_STORES" |
| <a id="s-3869830d71"></a>`name` | "RIVERHOG_ARCHIVE_STORES" |
| <a id="s-ff15d2537d"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-b129044273"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_STORES](../../../evidence/sources.md#src-b4408185b7) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/13/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `values.get('RIVERHOG_ARCHIVE_STORES', 'archive')` |

### Machine authority

- `/external_contract/configuration_environment/38`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ef2d6f798b2800773214ef8ec596bd86c82e0549426086e9db4c6f20a3e7d4c -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_STORES",
  "name": "RIVERHOG_ARCHIVE_STORES",
  "owner": "riverhog-server"
}
```
