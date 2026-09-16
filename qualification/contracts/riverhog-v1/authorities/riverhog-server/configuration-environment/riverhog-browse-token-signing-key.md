# RIVERHOG_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-browse-token-signing-key:7b9b9558ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-71aef6f25d"></a>

| Field | Value |
|---|---|
| <a id="s-a3df3866fb"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-3fe8602d6c"></a>`default_expressions` | `["''"]` |
| <a id="s-ffb1f55176"></a>`id` | `"riverhog-server:environment:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY"` |
| <a id="s-cf559f5a8f"></a>`input_shape` | `"environment-string"` |
| <a id="s-8019296a33"></a>`name` | `"RIVERHOG_BROWSE_TOKEN_SIGNING_KEY"` |
| <a id="s-796b4f0f4b"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-af0eb2df15"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY](../../../evidence/sources.md#src-70d116fdbc) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_BROWSE_TOKEN_SIGNING_KEY', '')` |

### Machine authority

- `/external_contract/configuration_environment/50`

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
