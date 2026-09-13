# STOVE0_API_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-api-token:27d1df0d0f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-30c0913544"></a>
| Field | Shape |
|---|---|
| <a id="s-d00e440158"></a>`consumers` | ["stove0-server"] |
| <a id="s-665a0384a8"></a>`default_expressions` | ["''"] |
| <a id="s-6b03089d6d"></a>`id` | "stove0-server:environment:STOVE0_API_TOKEN" |
| <a id="s-0c7781c780"></a>`input_shape` | "environment-string" |
| <a id="s-4fdea0c27a"></a>`name` | "STOVE0_API_TOKEN" |
| <a id="s-8ee07180a7"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-82758651ec"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_API_TOKEN](../../../evidence/sources.md#src-b54731a17b) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, '')` |

### Machine authority

- `/external_contract/configuration_environment/229`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 66513233348bdbfc9093c7224ffea7fedf06b357880648ad42aef96544b97b15 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_API_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_API_TOKEN",
  "owner": "stove0-server"
}
```
