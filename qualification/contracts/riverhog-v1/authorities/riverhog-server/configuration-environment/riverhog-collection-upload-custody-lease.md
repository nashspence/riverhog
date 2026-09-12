# RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-collection-upload-custody-lease:4d7aa972b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-9857458ffa"></a>
| Field | Shape |
|---|---|
| <a id="s-224e6d19dc"></a>`classification` | "runtime" |
| <a id="s-fe693377ae"></a>`consumers` | ["riverhog-server"] |
| <a id="s-74c66258c4"></a>`disposition` | "contractual" |
| <a id="s-8cbdf403cb"></a>`id` | "riverhog-server:environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE" |
| <a id="s-03c589148c"></a>`name` | "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE" |
| <a id="s-0666993cd6"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](#s-9857458ffa) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-58487299ac"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-e1c7e3afbb"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../../../evidence/sources.md#src-4cfc3a8358) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE', '1h')` |

### Machine authority

- `/external_contract/configuration_environment/51`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4da3b83c2fc03d04132942e5023f981d664b40aa4075114e2a92361c0de70020 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE",
  "name": "RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE",
  "owner": "riverhog-server"
}
```
