# RIVERHOG_EVENT_SOURCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-event-source:f2d0fde066 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4d9e9baa2d"></a>

| Field | Value |
|---|---|
| <a id="s-ac1870ac7e"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-dbeac2fd91"></a>`default_expressions` | `["'urn:riverhog'"]` |
| <a id="s-88ec9d4401"></a>`id` | `"riverhog-server:environment:RIVERHOG_EVENT_SOURCE"` |
| <a id="s-088ce46815"></a>`input_shape` | `"environment-string"` |
| <a id="s-4eb4f92e09"></a>`name` | `"RIVERHOG_EVENT_SOURCE"` |
| <a id="s-090693892d"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-367cf708b1"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_EVENT_SOURCE](../../../evidence/sources/authorities.md#src-cc7e1cd9d7) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_EVENT_SOURCE', 'urn:riverhog')` |

### Machine authority

- `/external_contract/configuration_environment/194`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09c8643350d173b66a3e672f6cc1efbfacde6afdca3c3521c7d84934ae53373b -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'urn:riverhog'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_EVENT_SOURCE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_EVENT_SOURCE",
  "owner": "riverhog-server"
}
```

</details>
