# STOVE0_TARGET_AUTHORITY_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-authority-batch-size:875d4123db -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ce7b3c0fe5"></a>

| Field | Value |
|---|---|
| <a id="s-b531aaadad"></a>`consumers` | `["stove0-server"]` |
| <a id="s-0b0de17673"></a>`default_expressions` | `["str(default)"]` |
| <a id="s-39fe167954"></a>`id` | `"stove0-server:environment:STOVE0_TARGET_AUTHORITY_BATCH_SIZE"` |
| <a id="s-dab7acf396"></a>`input_shape` | `"environment-string"` |
| <a id="s-92dba1f18c"></a>`name` | `"STOVE0_TARGET_AUTHORITY_BATCH_SIZE"` |
| <a id="s-5460aac20b"></a>`owner` | `"stove0-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_TARGET_AUTHORITY_BATCH_SIZE"; consumers=["stove0-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_TARGET_AUTHORITY_BATCH_SIZE](#s-ce7b3c0fe5) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-322ed5edbd"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-6655abbe63"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../../../evidence/sources/authorities.md#src-86bb163042) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_integer](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(name, str(default))` |

### Machine authority

- `/external_contract/configuration_environment/245`

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
