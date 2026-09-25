# STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-target-support:stove0-target-terminal-state-retention-seconds:a017562b3a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-965179fc46"></a>

| Field | Value |
|---|---|
| <a id="s-4433b3c434"></a>`consumers` | `["stove0-target-support"]` |
| <a id="s-83ba75a7bc"></a>`default_expressions` | `["str(DEFAULT_TERMINAL_STATE_RETENTION_SECONDS)"]` |
| <a id="s-efd9f29cdd"></a>`id` | `"stove0-target-support:environment:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"` |
| <a id="s-7284882569"></a>`input_shape` | `"environment-string"` |
| <a id="s-4dd5bfca84"></a>`name` | `"STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"` |
| <a id="s-7d68affc90"></a>`owner` | `"stove0-target-support"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"; consumers=["stove0-target-support"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](#s-965179fc46) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f46ec3eda4"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-3a837c5567"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

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

- `/external_contract/configuration_environment/120`

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
