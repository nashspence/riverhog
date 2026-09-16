# STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-target-support:stove0-target-terminal-state-retention-seconds:b0d7d507c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b8e9029e16"></a>

| Field | Value |
|---|---|
| <a id="s-6389ef5704"></a>`consumers` | `["stove0-target-support"]` |
| <a id="s-e5e91bdd9c"></a>`default_expressions` | `["str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS)"]` |
| <a id="s-9ef9b8dc24"></a>`id` | `"stove0-target-support:environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"` |
| <a id="s-163925c5a6"></a>`input_shape` | `"environment-string"` |
| <a id="s-f93b3cfeb9"></a>`name` | `"STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"` |
| <a id="s-338261e172"></a>`owner` | `"stove0-target-support"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"; consumers=["stove0-target-support"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](#s-b8e9029e16) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-0fcc1d5471"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-5eb1887034"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-target-support:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../../../evidence/sources.md#src-a4a7b5aed4) — `reference/stove0/packages/target-support/src/stove0_target_support/configuration.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-target-support` | `reference/stove0/packages/target-support/src/stove0_target_support/configuration.py` | `values.get(TARGET_TERMINAL_STATE_RETENTION_ENV, str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS))` |

### Machine authority

- `/external_contract/configuration_environment/249`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41082d40385d615f41b20ba8d9bacf238223435ad195d3ed49653068c68d2317 -->

```json
{
  "consumers": [
    "stove0-target-support"
  ],
  "default_expressions": [
    "str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS)"
  ],
  "id": "stove0-target-support:environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS",
  "input_shape": "environment-string",
  "name": "STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS",
  "owner": "stove0-target-support"
}
```

</details>
