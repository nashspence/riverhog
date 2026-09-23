# RIVERHOG_PROVENANCE_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-provenance:riverhog-provenance-state-home:667bf75650 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-93311950fd"></a>

| Field | Value |
|---|---|
| <a id="s-2f4d41b4b3"></a>`consumers` | `["riverhog-provenance"]` |
| <a id="s-1ab26ba46c"></a>`default_expressions` | `["unset"]` |
| <a id="s-3f335b0525"></a>`id` | `"riverhog-provenance:environment:RIVERHOG_PROVENANCE_STATE_HOME"` |
| <a id="s-f415c8792e"></a>`input_shape` | `"environment-string"` |
| <a id="s-5f10e92d26"></a>`name` | `"RIVERHOG_PROVENANCE_STATE_HOME"` |
| <a id="s-ca63a482d2"></a>`owner` | `"riverhog-provenance"` |

## Governing policies

- <a id="pa-3c7ee86a80"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-provenance:RIVERHOG_PROVENANCE_STATE_HOME](../../../evidence/sources/authorities.md#src-9fe04e9d42) — [packages/riverhog-provenance/src/riverhog\_provenance/identity.py::\_user\_state\_root](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/identity.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-provenance` | [packages/riverhog-provenance/src/riverhog\_provenance/identity.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/identity.py) | `os.getenv('RIVERHOG_PROVENANCE_STATE_HOME')` |

### Machine authority

- `/external_contract/configuration_environment/167`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 216534df74d09e478d44ab6b623cb2fadd540d11dffe4684dd4be34ff6563d8f -->

```json
{
  "consumers": [
    "riverhog-provenance"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-provenance:environment:RIVERHOG_PROVENANCE_STATE_HOME",
  "input_shape": "environment-string",
  "name": "RIVERHOG_PROVENANCE_STATE_HOME",
  "owner": "riverhog-provenance"
}
```

</details>
