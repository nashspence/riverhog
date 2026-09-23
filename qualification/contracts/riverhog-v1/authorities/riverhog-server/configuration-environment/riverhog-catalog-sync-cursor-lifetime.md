# RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-cursor-lifetime:485f05e585 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5cf617c7a1"></a>

| Field | Value |
|---|---|
| <a id="s-3bdfe4a37f"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-f1e8e66179"></a>`default_expressions` | `["'24h'"]` |
| <a id="s-fc8f9e3eb4"></a>`id` | `"riverhog-server:environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME"` |
| <a id="s-ee45c84e11"></a>`input_shape` | `"environment-string"` |
| <a id="s-e188630524"></a>`name` | `"RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME"` |
| <a id="s-7c1ae4c87e"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-04afb1d081"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME](../../../evidence/sources/authorities.md#src-afa71403d4) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME', '24h')` |

### Machine authority

- `/external_contract/configuration_environment/186`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72c266eb336e70afa2436ce48d879e5d129ea88a9e1a1eb3841a56236a27dd24 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'24h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME",
  "input_shape": "environment-string",
  "name": "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME",
  "owner": "riverhog-server"
}
```

</details>
