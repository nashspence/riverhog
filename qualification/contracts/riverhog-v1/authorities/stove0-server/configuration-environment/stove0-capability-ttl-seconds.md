# STOVE0_CAPABILITY_TTL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-capability-ttl-seconds:7240e62030 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-2da04c48df) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-bd9bd4c428"></a>
| Field | Shape |
|---|---|
| <a id="s-95b15c3629"></a>`classification` | "runtime" |
| <a id="s-46dd00a5c8"></a>`consumers` | ["stove0-server"] |
| <a id="s-bf025abcc7"></a>`disposition` | "contractual" |
| <a id="s-d7df8d8378"></a>`id` | "stove0-server:environment:STOVE0_CAPABILITY_TTL_SECONDS" |
| <a id="s-2f10d77123"></a>`name` | "STOVE0_CAPABILITY_TTL_SECONDS" |
| <a id="s-1bda924b26"></a>`owner` | "stove0-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_CAPABILITY_TTL_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_CAPABILITY_TTL_SECONDS](#s-bd9bd4c428) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-d82e73de51"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-8946dfa506"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_CAPABILITY_TTL_SECONDS](../../../evidence/sources.md#src-0e9a09a14d) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/28/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_integer(values, 'STOVE0_CAPABILITY_TTL_SECONDS', 900, minimum=30)` |

### Machine authority

- `/external_contract/configuration_environment/113`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47c23afbd5881d027225cba95fe6085eeb385eaf084a4ff486e10701c41d94a1 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_CAPABILITY_TTL_SECONDS",
  "name": "STOVE0_CAPABILITY_TTL_SECONDS",
  "owner": "stove0-server"
}
```
