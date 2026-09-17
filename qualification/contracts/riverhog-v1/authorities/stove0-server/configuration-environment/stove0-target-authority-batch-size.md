# STOVE0_TARGET_AUTHORITY_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-authority-batch-size:0f098326ad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-354f5bd537"></a>

| Field | Value |
|---|---|
| <a id="s-e106220cca"></a>`consumers` | `["stove0-server"]` |
| <a id="s-4584971624"></a>`default_expressions` | `["str(default)"]` |
| <a id="s-b2a021a367"></a>`id` | `"stove0-server:environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE"` |
| <a id="s-5912be7cb1"></a>`input_shape` | `"environment-string"` |
| <a id="s-99c639d07e"></a>`name` | `"STOVE0_TARGET_AUTHORITY_BATCH_SIZE"` |
| <a id="s-4f78a82d30"></a>`owner` | `"stove0-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_TARGET_AUTHORITY_BATCH_SIZE"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_AUTHORITY_BATCH_SIZE](#s-354f5bd537) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-fa8bdb5423"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-69a58f838c"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../../../evidence/sources/authorities.md#src-86bb163042) — [reference/stove0/application/server/src/stove0\_core/runtime\_config.py::\_integer](../../../../../../reference/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [reference/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../reference/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, str(default))` |

### Machine authority

- `/external_contract/configuration_environment/243`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c71614c15e2f08d952f4a6f4121fadd18e5d414e4b56379062c93ceb874aba0c -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "str(default)"
  ],
  "id": "stove0-server:environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE",
  "input_shape": "environment-string",
  "name": "STOVE0_TARGET_AUTHORITY_BATCH_SIZE",
  "owner": "stove0-server"
}
```

</details>
