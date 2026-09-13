# RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-max-range-bytes:c987c1b891 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3efbdd0d50"></a>
| Field | Shape |
|---|---|
| <a id="s-e5a8456801"></a>`consumers` | ["riverhog-server"] |
| <a id="s-7044fe0ae8"></a>`default_expressions` | ["unset"] |
| <a id="s-c4e21ab4c8"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES" |
| <a id="s-ee31f3135d"></a>`input_shape` | "environment-string" |
| <a id="s-8bf9d9fca2"></a>`name` | "RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES" |
| <a id="s-33c76d2bc9"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](#s-3efbdd0d50) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-1bbcc94e2d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-57632dbdea"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../../../evidence/sources.md#src-1e662f4cb2) — `riverhog/src/riverhog_core/pack_retrieval.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/pack_retrieval.py` | `values.get(global_name)` |

### Machine authority

- `/external_contract/configuration_environment/78`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 81cb306ced57919ff107782ba1a6a1b78572e329aa77645d4f82a856c83e2d6c -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES",
  "owner": "riverhog-server"
}
```
