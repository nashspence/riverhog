# RIVERHOG_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-config:9a322ea0f0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-bd9bd4c428"></a>

| Field | Value |
|---|---|
| <a id="s-46dd00a5c8"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-a39501b2d0"></a>`default_expressions` | `["''","unset"]` |
| <a id="s-d7df8d8378"></a>`id` | `"riverhog-server:environment:RIVERHOG_CONFIG"` |
| <a id="s-409705dbe5"></a>`input_shape` | `"environment-string"` |
| <a id="s-2f10d77123"></a>`name` | `"RIVERHOG_CONFIG"` |
| <a id="s-1bda924b26"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-f863fb0d36"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CONFIG](../../../evidence/sources/authorities.md#src-7a5aba8378) — [riverhog/src/riverhog\_api/app.py::main](../../../../../../riverhog/src/riverhog_api/app.py); [riverhog/src/riverhog\_core/runtime\_document.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_document.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_api/app.py](../../../../../../riverhog/src/riverhog_api/app.py) | `os.environ['RIVERHOG_CONFIG']` |
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_document.py](../../../../../../riverhog/src/riverhog_core/runtime_document.py) | `os.environ.get('RIVERHOG_CONFIG', '')` |

### Machine authority

- `/external_contract/configuration_environment/113`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68970355c67887f5f5fa3589917ded75c0f640ad260b911ab56076df6faf8041 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''",
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_CONFIG",
  "input_shape": "environment-string",
  "name": "RIVERHOG_CONFIG",
  "owner": "riverhog-server"
}
```

</details>
