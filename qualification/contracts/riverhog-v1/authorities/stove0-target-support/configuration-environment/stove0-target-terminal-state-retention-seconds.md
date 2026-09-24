# STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-target-support:stove0-target-terminal-state-retention-seconds:89c114eb6f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ebe4e2c29d"></a>

| Field | Value |
|---|---|
| <a id="s-411d524a1d"></a>`consumers` | `["stove0-target-support"]` |
| <a id="s-0e69a309d8"></a>`default_expressions` | `["str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS)"]` |
| <a id="s-50362410c1"></a>`id` | `"stove0-target-support:environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"` |
| <a id="s-6aa24ea604"></a>`input_shape` | `"environment-string"` |
| <a id="s-a759ad44ac"></a>`name` | `"STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"` |
| <a id="s-ebd0787666"></a>`owner` | `"stove0-target-support"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"; consumers=["stove0-target-support"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](#s-ebe4e2c29d) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-92d0f2da2e"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-fcc5c4e99b"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

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

- `/external_contract/configuration_environment/248`

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
