# RIVERHOG_PUBLIC_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-public-base-url:3368a3e16a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-a44ff15aa7"></a>

| Field | Value |
|---|---|
| <a id="s-cbfaedd8f7"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-2b9491bcda"></a>`default_expressions` | `["''"]` |
| <a id="s-2908730b97"></a>`id` | `"riverhog-server:environment:RIVERHOG_PUBLIC_BASE_URL"` |
| <a id="s-96390f5c80"></a>`input_shape` | `"environment-string"` |
| <a id="s-394d41cd98"></a>`name` | `"RIVERHOG_PUBLIC_BASE_URL"` |
| <a id="s-436907c2b0"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-0e6abe0441"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_PUBLIC_BASE_URL](../../../evidence/sources/authorities.md#src-fbbd173a78) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_PUBLIC_BASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/67`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 018748529abab2949264a62afb4e6a5808534fcb71d6b6d9b7b08d353fd02ebe -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-server:environment:RIVERHOG_PUBLIC_BASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_PUBLIC_BASE_URL",
  "owner": "riverhog-server"
}
```

</details>
