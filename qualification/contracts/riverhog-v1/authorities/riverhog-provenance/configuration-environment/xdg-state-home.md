# XDG_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-provenance:xdg-state-home:75441a7ea6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-522054ba96"></a>

| Field | Value |
|---|---|
| <a id="s-10b1bd0fd7"></a>`consumers` | `["riverhog-provenance"]` |
| <a id="s-4987bd6783"></a>`default_expressions` | `["unset"]` |
| <a id="s-881365a784"></a>`id` | `"riverhog-provenance:environment:XDG_STATE_HOME"` |
| <a id="s-43682c84fe"></a>`input_shape` | `"environment-string"` |
| <a id="s-020147690b"></a>`name` | `"XDG_STATE_HOME"` |
| <a id="s-3bb548651c"></a>`owner` | `"riverhog-provenance"` |

## Governing policies

- <a id="pa-4ced077b91"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-provenance:XDG_STATE_HOME](../../../evidence/sources/authorities.md#src-fb8d65585b) — [packages/riverhog-provenance/src/riverhog\_provenance/identity.py::\_user\_state\_root](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/identity.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-provenance` | [packages/riverhog-provenance/src/riverhog\_provenance/identity.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/identity.py) | `os.getenv('XDG_STATE_HOME')` |

### Machine authority

- `/external_contract/configuration_environment/168`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44e545ff62be823f0c16cf187dc89d2b704134b09f3ed24fd325bf3bba5b4388 -->

```json
{
  "consumers": [
    "riverhog-provenance"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-provenance:environment:XDG_STATE_HOME",
  "input_shape": "environment-string",
  "name": "XDG_STATE_HOME",
  "owner": "riverhog-provenance"
}
```

</details>
