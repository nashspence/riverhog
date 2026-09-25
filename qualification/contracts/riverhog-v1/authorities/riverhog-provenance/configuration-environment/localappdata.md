# LOCALAPPDATA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-provenance:localappdata:b989c62f26 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6cd1741870"></a>

| Field | Value |
|---|---|
| <a id="s-f89c1c3048"></a>`consumers` | `["riverhog-provenance"]` |
| <a id="s-4209677621"></a>`default_expressions` | `["unset"]` |
| <a id="s-ee9ed3533d"></a>`id` | `"riverhog-provenance:environment:LOCALAPPDATA"` |
| <a id="s-42c7b7c437"></a>`input_shape` | `"environment-string"` |
| <a id="s-7027ddbe71"></a>`name` | `"LOCALAPPDATA"` |
| <a id="s-5b4c4db475"></a>`owner` | `"riverhog-provenance"` |

## Governing policies

- <a id="pa-93ca85c42d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-provenance:LOCALAPPDATA](../../../evidence/sources/authorities.md#src-d5c0e21c89) — [packages/riverhog-provenance/src/riverhog\_provenance/identity.py::\_user\_state\_root](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/identity.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-provenance` | [packages/riverhog-provenance/src/riverhog\_provenance/identity.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/identity.py) | `os.getenv('LOCALAPPDATA')` |

### Machine authority

- `/external_contract/configuration_environment/110`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6eecff273ae7c443f5e6bff5d6d2e70bc64b5b3442e25d6a35baec92679b104a -->

```json
{
  "consumers": [
    "riverhog-provenance"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-provenance:environment:LOCALAPPDATA",
  "input_shape": "environment-string",
  "name": "LOCALAPPDATA",
  "owner": "riverhog-provenance"
}
```

</details>
