# STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-operational-state-retention-seconds:63b375a30c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-2da04c48df) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-03d76831f6"></a>
| Field | Shape |
|---|---|
| <a id="s-945cad7561"></a>`classification` | "runtime" |
| <a id="s-9d176682fc"></a>`consumers` | ["stove0-server"] |
| <a id="s-de476f4a9e"></a>`disposition` | "contractual" |
| <a id="s-70fa9907ac"></a>`id` | "stove0-server:environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS" |
| <a id="s-709f3f0d7c"></a>`name` | "STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS" |
| <a id="s-3cae61146e"></a>`owner` | "stove0-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](#s-03d76831f6) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-5445626c90"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-dd5603c131"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](../../../evidence/sources.md#src-024a6340ee) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/28/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_integer(values, 'STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS', DEFAULT_OPERATIONAL_STATE_RETENTION_SECONDS, minimum=1)` |

### Machine authority

- `/external_contract/configuration_environment/117`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f12344747655afd7a5e119cdf3bc584204e064d1a6864fcd77e826ee36658733 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS",
  "name": "STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS",
  "owner": "stove0-server"
}
```
