# STOVE0_WORKSPACE_ASSURANCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-workspace-assurance:cfeb995acb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ebe4e2c29d"></a>
| Field | Shape |
|---|---|
| <a id="s-411d524a1d"></a>`consumers` | ["stove0-server"] |
| <a id="s-0e69a309d8"></a>`default_expressions` | ["'encrypted'"] |
| <a id="s-50362410c1"></a>`id` | "stove0-server:environment:STOVE0_WORKSPACE_ASSURANCE" |
| <a id="s-6aa24ea604"></a>`input_shape` | "environment-string" |
| <a id="s-a759ad44ac"></a>`name` | "STOVE0_WORKSPACE_ASSURANCE" |
| <a id="s-ebd0787666"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-06805f11bc"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_WORKSPACE_ASSURANCE](../../../evidence/sources.md#src-26ac73d782) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get('STOVE0_WORKSPACE_ASSURANCE', 'encrypted')` |

### Machine authority

- `/external_contract/configuration_environment/248`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0b85062bc902bcc1660bd9ad34ab0d41a4741779b0768042d120d401b9815f8 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "'encrypted'"
  ],
  "id": "stove0-server:environment:STOVE0_WORKSPACE_ASSURANCE",
  "input_shape": "environment-string",
  "name": "STOVE0_WORKSPACE_ASSURANCE",
  "owner": "stove0-server"
}
```
