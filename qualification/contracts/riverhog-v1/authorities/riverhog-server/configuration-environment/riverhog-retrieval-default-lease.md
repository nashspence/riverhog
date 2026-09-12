# RIVERHOG_RETRIEVAL_DEFAULT_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-default-lease:7c7011fa4b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-63134bf139"></a>
| Field | Shape |
|---|---|
| <a id="s-9fab94a2a0"></a>`classification` | "runtime" |
| <a id="s-7a2481d8f8"></a>`consumers` | ["riverhog-server"] |
| <a id="s-62cef3aeda"></a>`disposition` | "contractual" |
| <a id="s-d1248cce4c"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE" |
| <a id="s-822be7b829"></a>`name` | "RIVERHOG_RETRIEVAL_DEFAULT_LEASE" |
| <a id="s-3c8df38f70"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_DEFAULT_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_DEFAULT_LEASE](#s-63134bf139) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-4fde224085"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-6512fa1522"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_DEFAULT_LEASE](../../../evidence/sources.md#src-4fd9a92a9d) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_DEFAULT_LEASE', '24h')` |

### Machine authority

- `/external_contract/configuration_environment/69`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a96eb66199e1045b26f61aba22bbaa7b838c03fdc66efb9b6bafbb8448c3eefe -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE",
  "name": "RIVERHOG_RETRIEVAL_DEFAULT_LEASE",
  "owner": "riverhog-server"
}
```
