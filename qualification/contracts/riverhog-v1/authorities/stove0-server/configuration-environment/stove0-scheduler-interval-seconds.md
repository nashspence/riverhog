# STOVE0_SCHEDULER_INTERVAL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-scheduler-interval-seconds:fa5752e04b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f1434e7972"></a>

| Field | Value |
|---|---|
| <a id="s-8b1bac247a"></a>`consumers` | `["stove0-server"]` |
| <a id="s-7885b7d9a8"></a>`default_expressions` | `["str(default)"]` |
| <a id="s-cd0ba82178"></a>`id` | `"stove0-server:environment:STOVE0_SCHEDULER_INTERVAL_SECONDS"` |
| <a id="s-cf63d91846"></a>`input_shape` | `"environment-string"` |
| <a id="s-3e0815658c"></a>`name` | `"STOVE0_SCHEDULER_INTERVAL_SECONDS"` |
| <a id="s-4651ce2447"></a>`owner` | `"stove0-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_SCHEDULER_INTERVAL_SECONDS"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_SCHEDULER_INTERVAL_SECONDS](#s-f1434e7972) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-1330e662a2"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-5a1d00743c"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_SCHEDULER_INTERVAL_SECONDS](../../../evidence/sources.md#src-1ba44b59b5) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, str(default))` |

### Machine authority

- `/external_contract/configuration_environment/241`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0758248a149d2eb4a31493de104fffde1aae50240bc0da902d199fcfcfa1b4f9 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "str(default)"
  ],
  "id": "stove0-server:environment:STOVE0_SCHEDULER_INTERVAL_SECONDS",
  "input_shape": "environment-string",
  "name": "STOVE0_SCHEDULER_INTERVAL_SECONDS",
  "owner": "stove0-server"
}
```

</details>
