# RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-page-size-max:26cbad8bc7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-71aef6f25d"></a>
| Field | Shape |
|---|---|
| <a id="s-9e915dff26"></a>`classification` | "runtime" |
| <a id="s-a3df3866fb"></a>`consumers` | ["riverhog-server"] |
| <a id="s-8559c5a713"></a>`disposition` | "contractual" |
| <a id="s-ffb1f55176"></a>`id` | "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX" |
| <a id="s-8019296a33"></a>`name` | "RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX" |
| <a id="s-796b4f0f4b"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](#s-71aef6f25d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-5cc465d5b4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-f18da5e94e"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../../../evidence/sources.md#src-65433124fb) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `_parse_int(os.getenv('RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX', str(CATALOG_SYNC_PAGE_SIZE_MAX)), name='RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX', minimum=1)` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX', str(CATALOG_SYNC_PAGE_SIZE_MAX))` |

### Machine authority

- `/external_contract/configuration_environment/50`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c525186b2de2768c424b3c00953992ed93d68acb218cdc8dc7dd49bd4a311411 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX",
  "name": "RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX",
  "owner": "riverhog-server"
}
```
