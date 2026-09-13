# RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-prepare-concurrency:4d56ae25b0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c26bd33aa8"></a>
| Field | Shape |
|---|---|
| <a id="s-cbfe80a062"></a>`consumers` | ["riverhog-server"] |
| <a id="s-8d23212c1a"></a>`default_expressions` | ["unset"] |
| <a id="s-a3f3fe3044"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY" |
| <a id="s-7d81361521"></a>`input_shape` | "environment-string" |
| <a id="s-501ec97f37"></a>`name` | "RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY" |
| <a id="s-06c79d612b"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](#s-c26bd33aa8) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-714e3ad98d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-ff4e4d0dec"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../../../evidence/sources.md#src-36a45a4230) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/40`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc19d7cc5faf560a00440b9402f2e14538b0278c30f7c8eea9e5d797b2c6bf82 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY",
  "owner": "riverhog-server"
}
```
