# RIVERHOG_BOOTSTRAP_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-bootstrap-token:d9f79786ef -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4ce7abb670"></a>

| Field | Value |
|---|---|
| <a id="s-ebe8a893a2"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-003bc4201d"></a>`default_expressions` | `["''"]` |
| <a id="s-f10ba9f7a8"></a>`id` | `"riverhog-server:environment:RIVERHOG_BOOTSTRAP_TOKEN"` |
| <a id="s-7aa187d92b"></a>`input_shape` | `"environment-string"` |
| <a id="s-51849146a8"></a>`name` | `"RIVERHOG_BOOTSTRAP_TOKEN"` |
| <a id="s-a7a04652dd"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-cddbd2d501"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_BOOTSTRAP_TOKEN](../../../evidence/sources/authorities.md#src-46bdbba342) — [riverhog/src/riverhog\_api/auth.py::authenticate\_token](../../../../../../riverhog/src/riverhog_api/auth.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_api/auth.py](../../../../../../riverhog/src/riverhog_api/auth.py) | `os.getenv(BOOTSTRAP_TOKEN_ENV, '')` |

### Machine authority

- `/external_contract/configuration_environment/182`

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
