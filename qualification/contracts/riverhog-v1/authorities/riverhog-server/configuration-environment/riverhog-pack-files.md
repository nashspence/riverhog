# RIVERHOG_PACK_FILES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-pack-files:a1b4d8ac59 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e9a01debe6"></a>

| Field | Value |
|---|---|
| <a id="s-ebb317ac70"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-68cbb4f4c7"></a>`default_expressions` | `["unset"]` |
| <a id="s-bd4e6d9d2a"></a>`id` | `"riverhog-server:environment:RIVERHOG_PACK_FILES"` |
| <a id="s-07cd635e83"></a>`input_shape` | `"environment-string"` |
| <a id="s-0b20ca18fe"></a>`name` | `"RIVERHOG_PACK_FILES"` |
| <a id="s-a3279fc613"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-5d8b856f8c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_PACK_FILES](../../../evidence/sources/authorities.md#src-7d59fe1eef) — [riverhog/src/riverhog\_core/collection\_plan.py::\_env\_int](../../../../../../riverhog/src/riverhog_core/collection_plan.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/collection\_plan.py](../../../../../../riverhog/src/riverhog_core/collection_plan.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/198`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a1373931e59e554060969427f31f1370f093cab111f0454e3670e8eb92b36e9 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_PACK_FILES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_PACK_FILES",
  "owner": "riverhog-server"
}
```

</details>
