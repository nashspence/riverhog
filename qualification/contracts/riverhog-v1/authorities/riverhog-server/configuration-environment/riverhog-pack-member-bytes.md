# RIVERHOG_PACK_MEMBER_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-pack-member-bytes:c2b5ee25d2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-51bdf3ea73"></a>
| Field | Shape |
|---|---|
| <a id="s-63e71fe829"></a>`classification` | "runtime" |
| <a id="s-46d6397232"></a>`consumers` | ["riverhog-server"] |
| <a id="s-7e738d072b"></a>`disposition` | "contractual" |
| <a id="s-8708489666"></a>`id` | "riverhog-server:environment:RIVERHOG_PACK_MEMBER_BYTES" |
| <a id="s-cb037aed18"></a>`name` | "RIVERHOG_PACK_MEMBER_BYTES" |
| <a id="s-869feebb7c"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_PACK_MEMBER_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_PACK_MEMBER_BYTES](#s-51bdf3ea73) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-89d60048a1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-fcefe4c562"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_PACK_MEMBER_BYTES](../../../evidence/sources.md#src-f8aff5063d) — `riverhog/src/riverhog_core/collection_plan.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/collection_plan.py` | `_env_bytes(values, 'RIVERHOG_PACK_MEMBER_BYTES', DEFAULT_PACK_MEMBER_BYTES)` |

### Machine authority

- `/external_contract/configuration_environment/60`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2a25b263a755e60a6ae876a3a74ac84edf9bd2a4c3620c27ec0bf88becaea85 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_PACK_MEMBER_BYTES",
  "name": "RIVERHOG_PACK_MEMBER_BYTES",
  "owner": "riverhog-server"
}
```
