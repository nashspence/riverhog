# RIVERHOG_ARCHIVE_STORES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-stores:2c1c0c6068 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-387607e502"></a>

| Field | Value |
|---|---|
| <a id="s-dd9424e6bc"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-43cf01e1d2"></a>`default_expressions` | `["'archive'"]` |
| <a id="s-84fa5e52b6"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_STORES"` |
| <a id="s-1939accc0e"></a>`input_shape` | `"environment-string"` |
| <a id="s-bd5b86f3b7"></a>`name` | `"RIVERHOG_ARCHIVE_STORES"` |
| <a id="s-c1e07431f9"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-1a6a6e7494"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_STORES](../../../evidence/sources/authorities.md#src-b4408185b7) — [riverhog/src/riverhog\_core/runtime\_config.py::\_parse\_archive\_stores](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `values.get('RIVERHOG_ARCHIVE_STORES', 'archive')` |

### Machine authority

- `/external_contract/configuration_environment/177`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7deb739b33be792436d183b57c32c48615a1b923426a1bc5a6a3bac827aaef8 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'archive'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_STORES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_STORES",
  "owner": "riverhog-server"
}
```

</details>
