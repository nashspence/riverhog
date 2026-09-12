# RIVERHOG_PACK_SOURCE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-pack-source-bytes:342ff0118b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a9033edf7a"></a>
| Field | Shape |
|---|---|
| <a id="s-28dec2f382"></a>`classification` | "runtime" |
| <a id="s-7c2ffa6bcc"></a>`consumers` | ["riverhog-server"] |
| <a id="s-447ba6b27e"></a>`disposition` | "contractual" |
| <a id="s-5b8d93af24"></a>`id` | "riverhog-server:environment:RIVERHOG_PACK_SOURCE_BYTES" |
| <a id="s-7e4a9547db"></a>`name` | "RIVERHOG_PACK_SOURCE_BYTES" |
| <a id="s-c53e7cc506"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_PACK_SOURCE_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_PACK_SOURCE_BYTES](#s-a9033edf7a) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-c030061899"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-c0a8231bd0"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_PACK_SOURCE_BYTES](../../../evidence/sources.md#src-b2ca118143) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `_env_bytes(values, 'RIVERHOG_PACK_SOURCE_BYTES', DEFAULT_PACK_SOURCE_BYTES)` |

### Machine authority

- `/external_contract/configuration_environment/61`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3694bcc79df93fd4af80ee3dd1eae20d16f5073c3d92defd1ddc8b4cb6b10838 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_PACK_SOURCE_BYTES",
  "name": "RIVERHOG_PACK_SOURCE_BYTES",
  "owner": "riverhog-server"
}
```
