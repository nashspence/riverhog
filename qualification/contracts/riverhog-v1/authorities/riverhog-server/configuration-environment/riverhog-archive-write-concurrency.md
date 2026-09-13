# RIVERHOG_ARCHIVE_WRITE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-write-concurrency:7b3b740ccc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c5d59cd44b"></a>
| Field | Shape |
|---|---|
| <a id="s-4676feab29"></a>`consumers` | ["riverhog-server"] |
| <a id="s-5123e633a7"></a>`default_expressions` | ["unset"] |
| <a id="s-8e21124594"></a>`id` | "riverhog-server:environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY" |
| <a id="s-22a7d99444"></a>`input_shape` | "environment-string" |
| <a id="s-0774de54c0"></a>`name` | "RIVERHOG_ARCHIVE_WRITE_CONCURRENCY" |
| <a id="s-020c7424d1"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_WRITE_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](#s-c5d59cd44b) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-e295f82bfc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-27837dfa77"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](../../../evidence/sources.md#src-66ed403dd1) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/46`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bab5ef3d79bca52592c61134bfff4096cd0800e3d29eca80f4a13982f2fb0b7d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_WRITE_CONCURRENCY",
  "owner": "riverhog-server"
}
```
