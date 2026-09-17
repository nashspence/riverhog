# RIVERHOG_BOOTSTRAP_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-bootstrap-token:9ef567a98d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6d67d56ce0"></a>

| Field | Value |
|---|---|
| <a id="s-1a3e8ba501"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-303afe34c5"></a>`default_expressions` | `["''"]` |
| <a id="s-c1495d444f"></a>`id` | `"riverhog-server:environment:RIVERHOG_BOOTSTRAP_TOKEN"` |
| <a id="s-a4570fd55b"></a>`input_shape` | `"environment-string"` |
| <a id="s-639a4bef7f"></a>`name` | `"RIVERHOG_BOOTSTRAP_TOKEN"` |
| <a id="s-a908c852ad"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-d994654b33"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_BOOTSTRAP_TOKEN](../../../evidence/sources.md#src-46bdbba342) — [riverhog/src/riverhog\_api/auth.py::authenticate\_token](../../../../../../riverhog/src/riverhog_api/auth.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_api/auth.py](../../../../../../riverhog/src/riverhog_api/auth.py) | `os.getenv(BOOTSTRAP_TOKEN_ENV, '')` |

### Machine authority

- `/external_contract/configuration_environment/48`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0f3e426f10316926eac62610b87f0ee3ab7449633de48f89a417e0cc5a7f424 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-server:environment:RIVERHOG_BOOTSTRAP_TOKEN",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BOOTSTRAP_TOKEN",
  "owner": "riverhog-server"
}
```

</details>
