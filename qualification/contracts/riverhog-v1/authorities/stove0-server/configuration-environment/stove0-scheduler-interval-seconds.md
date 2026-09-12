# STOVE0_SCHEDULER_INTERVAL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-scheduler-interval-seconds:4d6f7df67a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-2da04c48df) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-7a1e93aef1"></a>
| Field | Shape |
|---|---|
| <a id="s-cd71426c29"></a>`classification` | "runtime" |
| <a id="s-31f233c17f"></a>`consumers` | ["stove0-server"] |
| <a id="s-1694a0d8e0"></a>`disposition` | "contractual" |
| <a id="s-8b573f8c6b"></a>`id` | "stove0-server:environment:STOVE0_SCHEDULER_INTERVAL_SECONDS" |
| <a id="s-be64f6ddb2"></a>`name` | "STOVE0_SCHEDULER_INTERVAL_SECONDS" |
| <a id="s-8669d543f0"></a>`owner` | "stove0-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_SCHEDULER_INTERVAL_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_SCHEDULER_INTERVAL_SECONDS](#s-7a1e93aef1) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-ddf1ec92a3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-1ee211c1b2"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_SCHEDULER_INTERVAL_SECONDS](../../../evidence/sources.md#src-1ba44b59b5) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/28/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_number(values, 'STOVE0_SCHEDULER_INTERVAL_SECONDS', 5.0, minimum=0.1)` |

### Machine authority

- `/external_contract/configuration_environment/119`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c7ab06aaffbbbab985fe7436fdc02aa97843d3dd76efff85365763f0d03658df -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_SCHEDULER_INTERVAL_SECONDS",
  "name": "STOVE0_SCHEDULER_INTERVAL_SECONDS",
  "owner": "stove0-server"
}
```
