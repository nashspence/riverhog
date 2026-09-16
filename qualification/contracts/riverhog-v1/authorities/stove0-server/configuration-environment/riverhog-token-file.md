# RIVERHOG_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:riverhog-token-file:72d089b843 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-fb95996087"></a>

| Field | Value |
|---|---|
| <a id="s-0cfaaf5cc7"></a>`consumers` | `["stove0-server"]` |
| <a id="s-6d7f32c41a"></a>`default_expressions` | `["''"]` |
| <a id="s-335c2b4e87"></a>`id` | `"stove0-server:environment:RIVERHOG_TOKEN_FILE"` |
| <a id="s-35470fd7f7"></a>`input_shape` | `"environment-string"` |
| <a id="s-081e60778b"></a>`name` | `"RIVERHOG_TOKEN_FILE"` |
| <a id="s-04c95d4c9b"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-a2ccfd1e6e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:RIVERHOG_TOKEN_FILE](../../../evidence/sources.md#src-03d80abe0b) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(f'{name}_FILE', '')` |

### Machine authority

- `/external_contract/configuration_environment/227`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8551ddea738d204ede9d98b311409599ecc652af40d176b6ef2e37b52f2aa483 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:RIVERHOG_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_TOKEN_FILE",
  "owner": "stove0-server"
}
```

</details>
