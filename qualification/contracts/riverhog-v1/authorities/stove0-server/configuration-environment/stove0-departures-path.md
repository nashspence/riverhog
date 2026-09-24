# STOVE0_DEPARTURES_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-departures-path:f731bd4d20 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e6ea24ca96"></a>

| Field | Value |
|---|---|
| <a id="s-bba0f0c4a2"></a>`consumers` | `["stove0-server"]` |
| <a id="s-891c749759"></a>`default_expressions` | `["''"]` |
| <a id="s-5c7cfbb798"></a>`id` | `"stove0-server:environment:STOVE0_DEPARTURES_PATH"` |
| <a id="s-dd7740d2f8"></a>`input_shape` | `"environment-string"` |
| <a id="s-35af37b988"></a>`name` | `"STOVE0_DEPARTURES_PATH"` |
| <a id="s-bee3607ec5"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-f6d36a69a7"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_DEPARTURES_PATH](../../../evidence/sources/authorities.md#src-4cb4e78a7b) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_departures](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get('STOVE0_DEPARTURES_PATH', '')` |

### Machine authority

- `/external_contract/configuration_environment/238`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cb308a4b9e99e0901602960fbfe43b19822706c4478f0cd0d9482f85de3dff6 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_DEPARTURES_PATH",
  "input_shape": "environment-string",
  "name": "STOVE0_DEPARTURES_PATH",
  "owner": "stove0-server"
}
```

</details>
