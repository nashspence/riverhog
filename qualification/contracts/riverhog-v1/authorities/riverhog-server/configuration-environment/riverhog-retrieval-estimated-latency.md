# RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-estimated-latency:753ecf9ed0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9978433062"></a>

| Field | Value |
|---|---|
| <a id="s-05ea931a8b"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-164afd85d0"></a>`default_expressions` | `["'48h'"]` |
| <a id="s-2d54433db7"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY"` |
| <a id="s-d3338c8191"></a>`input_shape` | `"environment-string"` |
| <a id="s-0ebd5e82c4"></a>`name` | `"RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY"` |
| <a id="s-22cb055147"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-ebe8bd3a31"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY](../../../evidence/sources/authorities.md#src-63c6f042b8) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY', '48h')` |

### Machine authority

- `/external_contract/configuration_environment/208`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c15b27ca1c8d6f3f14933e08353cbeeeb2e0d21c9291149054a6a11a7b348ec -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'48h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY",
  "owner": "riverhog-server"
}
```

</details>
