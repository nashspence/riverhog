# RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-ingress-source-read-chunk-bytes:e4e966479a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-bff615c87e"></a>
| Field | Shape |
|---|---|
| <a id="s-b6aa311db5"></a>`consumers` | ["riverhog-server"] |
| <a id="s-62067d9431"></a>`default_expressions` | ["unset"] |
| <a id="s-27e6fb1e12"></a>`id` | "riverhog-server:environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES" |
| <a id="s-1e94bb914a"></a>`input_shape` | "environment-string" |
| <a id="s-6be1a5a47c"></a>`name` | "RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES" |
| <a id="s-cf178005dd"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](#s-bff615c87e) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-57dd29d8ce"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-26e3a9ea28"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](../../../evidence/sources.md#src-bf9ecd1b0a) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/62`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9c9d557b9ffcdf9e31ed932e3b71ceca8d706b22a0700f7706b9c3c15e21b4e -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES",
  "owner": "riverhog-server"
}
```
