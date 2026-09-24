# RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-raw-volume-plaintext-bytes:91086be666 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e73f26dd50"></a>

| Field | Value |
|---|---|
| <a id="s-aca441222c"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-ac107dc046"></a>`default_expressions` | `["unset"]` |
| <a id="s-268e9e6049"></a>`id` | `"riverhog-server:environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES"` |
| <a id="s-4630333763"></a>`input_shape` | `"environment-string"` |
| <a id="s-f95cc39852"></a>`name` | `"RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES"` |
| <a id="s-b3f721f522"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](#s-e73f26dd50) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-13d16b7043"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-2fa08b528a"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](../../../evidence/sources/authorities.md#src-3fb717c8c3) — [riverhog/src/riverhog\_core/collection\_plan.py::\_env\_bytes](../../../../../../riverhog/src/riverhog_core/collection_plan.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/collection\_plan.py](../../../../../../riverhog/src/riverhog_core/collection_plan.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/201`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ecd7a5add6f3383735e5976362e68479aca3499d02419c05db36e7c1f4fac766 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES",
  "owner": "riverhog-server"
}
```

</details>
