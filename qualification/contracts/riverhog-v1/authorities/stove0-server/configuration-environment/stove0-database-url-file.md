# STOVE0_DATABASE_URL_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-database-url-file:e3d134153b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4773e132ed"></a>

| Field | Value |
|---|---|
| <a id="s-1d221a1146"></a>`consumers` | `["stove0-server"]` |
| <a id="s-2dc3e8cd70"></a>`default_expressions` | `["''"]` |
| <a id="s-f1ecf53c8a"></a>`id` | `"stove0-server:environment:STOVE0_DATABASE_URL_FILE"` |
| <a id="s-3f7fbc0057"></a>`input_shape` | `"environment-string"` |
| <a id="s-596e3e1a3e"></a>`name` | `"STOVE0_DATABASE_URL_FILE"` |
| <a id="s-7626cc0029"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-6a704d51e7"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_DATABASE_URL_FILE](../../../evidence/sources/authorities.md#src-6a24b678de) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_secret](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(f'{name}_FILE', '')` |

### Machine authority

- `/external_contract/configuration_environment/236`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30417f03a93ff11f155e96ff018c30b2dea981797700683ea6421cd40394df7f -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_DATABASE_URL_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_DATABASE_URL_FILE",
  "owner": "stove0-server"
}
```

</details>
