# STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-target-support:stove0-target-terminal-state-retention-seconds:752fba5e89 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-37d4154806"></a>

| Field | Value |
|---|---|
| <a id="s-3a95964586"></a>`consumers` | `["stove0-target-support"]` |
| <a id="s-a06ebcd7f7"></a>`default_expressions` | `["str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS)"]` |
| <a id="s-a7490fddb1"></a>`id` | `"stove0-target-support:environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"` |
| <a id="s-f0104dac03"></a>`input_shape` | `"environment-string"` |
| <a id="s-19bb9dbcb4"></a>`name` | `"STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"` |
| <a id="s-814e1b222c"></a>`owner` | `"stove0-target-support"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"; consumers=["stove0-target-support"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](#s-37d4154806) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1e43cd3bb3"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-f02445be5b"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-target-support:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../../../evidence/sources/authorities.md#src-a4a7b5aed4) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/configuration.py::terminal\_state\_retention\_seconds](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/configuration.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-target-support` | [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/configuration.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/configuration.py) | `values.get(TARGET_TERMINAL_STATE_RETENTION_ENV, str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS))` |

### Machine authority

- `/external_contract/configuration_environment/250`

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
