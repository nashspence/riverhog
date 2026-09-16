# STOVE0_DATABASE_URL_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-database-url-file:c4372f9c03 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d19efb74d1"></a>

| Field | Value |
|---|---|
| <a id="s-decf158927"></a>`consumers` | `["stove0-server"]` |
| <a id="s-93cce4292e"></a>`default_expressions` | `["''"]` |
| <a id="s-7d623ddfb5"></a>`id` | `"stove0-server:environment:STOVE0_DATABASE_URL_FILE"` |
| <a id="s-a92824bed5"></a>`input_shape` | `"environment-string"` |
| <a id="s-c8688fc9d6"></a>`name` | `"STOVE0_DATABASE_URL_FILE"` |
| <a id="s-511e086952"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-c5f66cbaea"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_DATABASE_URL_FILE](../../../evidence/sources.md#src-6a24b678de) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(f'{name}_FILE', '')` |

### Machine authority

- `/external_contract/configuration_environment/237`

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
