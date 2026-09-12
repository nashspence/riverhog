# RIVERHOG_RETRIEVAL_PENDING_TIMEOUT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-pending-timeout:b62929e81c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-e17f28d8d8"></a>
| Field | Shape |
|---|---|
| <a id="s-d9611f9a6b"></a>`classification` | "runtime" |
| <a id="s-a8f98984dc"></a>`consumers` | ["riverhog-server"] |
| <a id="s-b2c7980dee"></a>`disposition` | "contractual" |
| <a id="s-e1a39d2288"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT" |
| <a id="s-72292b5725"></a>`name` | "RIVERHOG_RETRIEVAL_PENDING_TIMEOUT" |
| <a id="s-5a79a41389"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_PENDING_TIMEOUT"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](#s-e17f28d8d8) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-aa7b7475b1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-6b0e41c173"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](../../../evidence/sources.md#src-01bd40bbb3) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_PENDING_TIMEOUT', '72h')` |

### Machine authority

- `/external_contract/configuration_environment/74`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9599e3d5adb6cfd5420539a56b00128db29592280f6157ca61afec6828f4db2 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT",
  "name": "RIVERHOG_RETRIEVAL_PENDING_TIMEOUT",
  "owner": "riverhog-server"
}
```
