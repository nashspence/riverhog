# RIVERHOG_RETRIEVAL_DEFAULT_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-default-lease:fd2fae3766 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-e17f28d8d8"></a>
| Field | Shape |
|---|---|
| <a id="s-a8f98984dc"></a>`consumers` | ["riverhog-server"] |
| <a id="s-1a8627f417"></a>`default_expressions` | ["'24h'"] |
| <a id="s-e1a39d2288"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE" |
| <a id="s-49efae42e7"></a>`input_shape` | "environment-string" |
| <a id="s-72292b5725"></a>`name` | "RIVERHOG_RETRIEVAL_DEFAULT_LEASE" |
| <a id="s-5a79a41389"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_DEFAULT_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_DEFAULT_LEASE](#s-e17f28d8d8) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-621de8ff05"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-f8c998e44f"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_DEFAULT_LEASE](../../../evidence/sources.md#src-4fd9a92a9d) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_DEFAULT_LEASE', '24h')` |

### Machine authority

- `/external_contract/configuration_environment/74`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebee515585743ad5f61b80538ce92ce2c35e7f3db5cd1d8f03d4080d3283a367 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'24h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_DEFAULT_LEASE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_DEFAULT_LEASE",
  "owner": "riverhog-server"
}
```
