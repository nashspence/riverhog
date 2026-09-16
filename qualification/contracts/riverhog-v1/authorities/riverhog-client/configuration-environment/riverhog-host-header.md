# RIVERHOG_HOST_HEADER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-host-header:3955860c5f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-9de9805f44"></a>

| Field | Value |
|---|---|
| <a id="s-f0e22869ab"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-d192d489c2"></a>`default_expressions` | `["''"]` |
| <a id="s-7c3da5fa0c"></a>`id` | `"riverhog-client:environment:RIVERHOG_HOST_HEADER"` |
| <a id="s-91a7ad7524"></a>`input_shape` | `"environment-string"` |
| <a id="s-81fae89078"></a>`name` | `"RIVERHOG_HOST_HEADER"` |
| <a id="s-683687933c"></a>`owner` | `"riverhog-client"` |

## Governing policies

- <a id="pa-8284ea03de"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_HOST_HEADER](../../../evidence/sources.md#src-9a32401dbf) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `os.getenv('RIVERHOG_HOST_HEADER', '')` |

### Machine authority

- `/external_contract/configuration_environment/17`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a9eafd78acb18a1522a4c2afb3917e19c0ac682975d1c79f9cdf537b3ec0a3d -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-client:environment:RIVERHOG_HOST_HEADER",
  "input_shape": "environment-string",
  "name": "RIVERHOG_HOST_HEADER",
  "owner": "riverhog-client"
}
```

</details>
