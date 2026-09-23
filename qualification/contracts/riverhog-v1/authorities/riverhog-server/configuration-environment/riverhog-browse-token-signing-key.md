# RIVERHOG_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-browse-token-signing-key:dd93e4fda8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4d873f87c7"></a>

| Field | Value |
|---|---|
| <a id="s-1b512f4642"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-4a866868af"></a>`default_expressions` | `["''"]` |
| <a id="s-1c5e68fc52"></a>`id` | `"riverhog-server:environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY"` |
| <a id="s-9c30e58ba4"></a>`input_shape` | `"environment-string"` |
| <a id="s-a11372d7df"></a>`name` | `"RIVERHOG_BROWSE_TOKEN_SIGNING_KEY"` |
| <a id="s-ea481a7a25"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-19657c21b9"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY](../../../evidence/sources/authorities.md#src-70d116fdbc) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_BROWSE_TOKEN_SIGNING_KEY', '')` |

### Machine authority

- `/external_contract/configuration_environment/184`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 011850a00b922fbbcd3dd02f78c1fe3e1b4449c6b3208b0e0fdafbc8d6d999e1 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-server:environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BROWSE_TOKEN_SIGNING_KEY",
  "owner": "riverhog-server"
}
```

</details>
