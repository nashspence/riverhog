# STOVE0_TARGET_AUTHORITY_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-authority-batch-size:5c05513710 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-2da04c48df) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-7109d2d380"></a>
| Field | Shape |
|---|---|
| <a id="s-ece5b8d6cf"></a>`classification` | "runtime" |
| <a id="s-cb034d2b66"></a>`consumers` | ["stove0-server"] |
| <a id="s-f1f7dd7198"></a>`disposition` | "contractual" |
| <a id="s-96289d1b72"></a>`id` | "stove0-server:environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE" |
| <a id="s-29050475a4"></a>`name` | "STOVE0_TARGET_AUTHORITY_BATCH_SIZE" |
| <a id="s-26a08e21d8"></a>`owner` | "stove0-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_TARGET_AUTHORITY_BATCH_SIZE"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_AUTHORITY_BATCH_SIZE](#s-7109d2d380) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-f24fdbc08b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-9b8b516c28"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../../../evidence/sources.md#src-86bb163042) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/28/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_integer(values, 'STOVE0_TARGET_AUTHORITY_BATCH_SIZE', 100, minimum=1, maximum=128)` |

### Machine authority

- `/external_contract/configuration_environment/121`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 335bc001a49a2e11a521dd09361cfb7e4b927edccfa906aead6df195e4185928 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE",
  "name": "STOVE0_TARGET_AUTHORITY_BATCH_SIZE",
  "owner": "stove0-server"
}
```
