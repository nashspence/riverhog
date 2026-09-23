# RIVERHOG_DATABASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-database-url:f8dfa4b945 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-fb2f551b7b"></a>

| Field | Value |
|---|---|
| <a id="s-0b33eff101"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-db5b575119"></a>`default_expressions` | `["''"]` |
| <a id="s-a91de35fd0"></a>`id` | `"riverhog-server:environment:RIVERHOG_DATABASE_URL"` |
| <a id="s-af63f043ee"></a>`input_shape` | `"environment-string"` |
| <a id="s-78304b2742"></a>`name` | `"RIVERHOG_DATABASE_URL"` |
| <a id="s-d27659d4cc"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-9367971fb8"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_DATABASE_URL](../../../evidence/sources/authorities.md#src-c84b874748) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_DATABASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/191`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59a96c089bc753ed30c2909868d5f4dbf440296dde0417ba1f192e8d68541c31 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-server:environment:RIVERHOG_DATABASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_DATABASE_URL",
  "owner": "riverhog-server"
}
```

</details>
