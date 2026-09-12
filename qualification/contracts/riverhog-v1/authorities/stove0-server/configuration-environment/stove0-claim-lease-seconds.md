# STOVE0_CLAIM_LEASE_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-claim-lease-seconds:8a27c2e88c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-2da04c48df) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-6d8ed99c25"></a>
| Field | Shape |
|---|---|
| <a id="s-ecd88f92aa"></a>`classification` | "runtime" |
| <a id="s-455d12eaca"></a>`consumers` | ["stove0-server"] |
| <a id="s-7140797b59"></a>`disposition` | "contractual" |
| <a id="s-70cf382471"></a>`id` | "stove0-server:environment:STOVE0_CLAIM_LEASE_SECONDS" |
| <a id="s-1b616d0548"></a>`name` | "STOVE0_CLAIM_LEASE_SECONDS" |
| <a id="s-8d577f9560"></a>`owner` | "stove0-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_CLAIM_LEASE_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_CLAIM_LEASE_SECONDS](#s-6d8ed99c25) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-e5a2b136cc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-cb1f3158cc"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_CLAIM_LEASE_SECONDS](../../../evidence/sources.md#src-1997f656ba) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/28/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_integer(values, 'STOVE0_CLAIM_LEASE_SECONDS', 1800, minimum=30)` |

### Machine authority

- `/external_contract/configuration_environment/114`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12a27d689c96f05219c3cec33ab770d9615a10399137611f7165cbb38cc91dd5 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_CLAIM_LEASE_SECONDS",
  "name": "STOVE0_CLAIM_LEASE_SECONDS",
  "owner": "stove0-server"
}
```
