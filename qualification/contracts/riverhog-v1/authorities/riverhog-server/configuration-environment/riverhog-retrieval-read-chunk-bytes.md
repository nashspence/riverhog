# RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-read-chunk-bytes:6695471d2c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-cbedc559e6"></a>
| Field | Shape |
|---|---|
| <a id="s-339b989f89"></a>`consumers` | ["riverhog-server"] |
| <a id="s-02e4d9f7d4"></a>`default_expressions` | ["unset"] |
| <a id="s-ab96b2e072"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES" |
| <a id="s-debabc62df"></a>`input_shape` | "environment-string" |
| <a id="s-7dead87c9e"></a>`name` | "RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES" |
| <a id="s-a99760a4aa"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](#s-cbedc559e6) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-7de3d85156"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-bd8d0283f9"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](../../../evidence/sources.md#src-236f08fcd9) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/82`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe5335a04030e2a75c97ae924ef38d6e1054085713d326e3333c3dae03adf30a -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES",
  "owner": "riverhog-server"
}
```
