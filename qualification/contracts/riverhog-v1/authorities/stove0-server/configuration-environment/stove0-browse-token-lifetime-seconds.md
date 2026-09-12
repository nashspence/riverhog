# STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-browse-token-lifetime-seconds:b1942923a2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-2da04c48df) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-7baa03212b"></a>
| Field | Shape |
|---|---|
| <a id="s-51ab368d2a"></a>`classification` | "runtime" |
| <a id="s-8d23c42891"></a>`consumers` | ["stove0-server"] |
| <a id="s-cd4aca2468"></a>`disposition` | "contractual" |
| <a id="s-0da1805b51"></a>`id` | "stove0-server:environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS" |
| <a id="s-aa9b56325e"></a>`name` | "STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS" |
| <a id="s-5216e992db"></a>`owner` | "stove0-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](#s-7baa03212b) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-22758e329d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-d0ea21d2c7"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../../../evidence/sources.md#src-8e79bc10a5) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/28/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_integer(values, 'STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS', 24 * 60 * 60, minimum=1)` |

### Machine authority

- `/external_contract/configuration_environment/111`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 308b3b1e83049b7e6030f99b0ea6ac24c851dace1f24916e48b5a63b3ce13e08 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS",
  "name": "STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS",
  "owner": "stove0-server"
}
```
