# RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-age-session-derivation-concurrency:c885e30963 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c4b672867c"></a>
| Field | Shape |
|---|---|
| <a id="s-5e51fca670"></a>`classification` | "runtime" |
| <a id="s-396c43f461"></a>`consumers` | ["riverhog-server"] |
| <a id="s-b1542684d5"></a>`disposition` | "contractual" |
| <a id="s-3f3f65b51c"></a>`id` | "riverhog-server:environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY" |
| <a id="s-019de684d7"></a>`name` | "RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY" |
| <a id="s-cd554621c2"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](#s-c4b672867c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-507971c846"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-9f8695a097"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](../../../evidence/sources.md#src-b3f6d318c3) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_int(values, 'RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY', DEFAULT_AGE_DERIVATION_CONCURRENCY)` |

### Machine authority

- `/external_contract/configuration_environment/31`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3be974aaeaef455dab8e0f520947141dcc06dc3eb9ed3cf07c40e113c7301576 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY",
  "name": "RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY",
  "owner": "riverhog-server"
}
```
