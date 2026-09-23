# RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-bootstrap-lifetime:f0c64d7ce6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-556ef016c2"></a>

| Field | Value |
|---|---|
| <a id="s-548453c111"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-99cea19f4f"></a>`default_expressions` | `["'7d'"]` |
| <a id="s-d75baad09a"></a>`id` | `"riverhog-server:environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME"` |
| <a id="s-83a8339220"></a>`input_shape` | `"environment-string"` |
| <a id="s-36b352e866"></a>`name` | `"RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME"` |
| <a id="s-5c39fe51b8"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-9b83ec85b5"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME](../../../evidence/sources/authorities.md#src-bb4e9ed43a) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME', '7d')` |

### Machine authority

- `/external_contract/configuration_environment/185`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60a9369be20017e0e474fec863c8b1dd23a29cb1bb68609dc4ce70a90574fe94 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'7d'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME",
  "input_shape": "environment-string",
  "name": "RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME",
  "owner": "riverhog-server"
}
```

</details>
