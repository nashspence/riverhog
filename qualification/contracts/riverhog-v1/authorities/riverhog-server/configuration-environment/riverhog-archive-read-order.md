# RIVERHOG_ARCHIVE_READ_ORDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-read-order:43422d392e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-bd4498fcba"></a>

| Field | Value |
|---|---|
| <a id="s-b833b4fe07"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-66e522afce"></a>`default_expressions` | `["','.join(names)"]` |
| <a id="s-5e1fc08ef8"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_READ_ORDER"` |
| <a id="s-5b74b79655"></a>`input_shape` | `"environment-string"` |
| <a id="s-9a7cb5cb3c"></a>`name` | `"RIVERHOG_ARCHIVE_READ_ORDER"` |
| <a id="s-7dd1a5ae30"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-1f05d4e5bd"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_READ_ORDER](../../../evidence/sources/authorities.md#src-b743528fa3) — [riverhog/src/riverhog\_core/runtime\_config.py::\_parse\_archive\_stores](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `values.get('RIVERHOG_ARCHIVE_READ_ORDER', ','.join(names))` |

### Machine authority

- `/external_contract/configuration_environment/175`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d344f2e08324fa8a060705c9bf114a5d1148b72ca3b0b19c9a3da881a83e16c -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "','.join(names)"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_READ_ORDER",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_READ_ORDER",
  "owner": "riverhog-server"
}
```

</details>
