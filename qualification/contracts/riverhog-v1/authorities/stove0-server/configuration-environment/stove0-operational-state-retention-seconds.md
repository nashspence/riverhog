# STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-operational-state-retention-seconds:6c314ec0c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a6b78ccc35"></a>
| Field | Shape |
|---|---|
| <a id="s-baa03590c9"></a>`consumers` | ["stove0-server"] |
| <a id="s-b93146a195"></a>`default_expressions` | ["str(default)"] |
| <a id="s-9426f68511"></a>`id` | "stove0-server:environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS" |
| <a id="s-583946b8e7"></a>`input_shape` | "environment-string" |
| <a id="s-a3b971cdef"></a>`name` | "STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS" |
| <a id="s-9d68c87baf"></a>`owner` | "stove0-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](#s-a6b78ccc35) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-93d40f9011"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-6d376549d7"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](../../../evidence/sources.md#src-024a6340ee) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, str(default))` |

### Machine authority

- `/external_contract/configuration_environment/239`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bc5adf110c61421515a7dac18604a36ee954a7383b9bf4ffe3418d2c0cf6839 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "str(default)"
  ],
  "id": "stove0-server:environment:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS",
  "input_shape": "environment-string",
  "name": "STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS",
  "owner": "stove0-server"
}
```
