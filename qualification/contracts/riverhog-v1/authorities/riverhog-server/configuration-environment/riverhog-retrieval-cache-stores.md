# RIVERHOG_RETRIEVAL_CACHE_STORES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-stores:94b77f8151 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-eb8aec9d18"></a>

| Field | Value |
|---|---|
| <a id="s-0a87e6a26d"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-e548f6ba22"></a>`default_expressions` | `["''"]` |
| <a id="s-919bb7b7ae"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_STORES"` |
| <a id="s-ef9ddf40ef"></a>`input_shape` | `"environment-string"` |
| <a id="s-1288e3cab6"></a>`name` | `"RIVERHOG_RETRIEVAL_CACHE_STORES"` |
| <a id="s-5aacd24817"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-05be055467"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_STORES](../../../evidence/sources/authorities.md#src-51dd06072e) — [riverhog/src/riverhog\_core/runtime\_config.py::\_parse\_retrieval\_cache\_stores](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `values.get('RIVERHOG_RETRIEVAL_CACHE_STORES', '')` |

### Machine authority

- `/external_contract/configuration_environment/204`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b646dd10a847f539b88e5724174ffdbe664941b2e0a5523a426f573318a16f7a -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_STORES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_CACHE_STORES",
  "owner": "riverhog-server"
}
```

</details>
