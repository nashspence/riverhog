# RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-prepare-concurrency:19c09f79bd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-3ad467ed2b"></a>

| Field | Value |
|---|---|
| <a id="s-fcf72def57"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-e5a282beb7"></a>`default_expressions` | `["unset"]` |
| <a id="s-addf591d9b"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY"` |
| <a id="s-d074d0c28a"></a>`input_shape` | `"environment-string"` |
| <a id="s-ebb9ec7233"></a>`name` | `"RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY"` |
| <a id="s-ab6d564bd6"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](#s-3ad467ed2b) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-85140da98e"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-f8b238f467"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../../../evidence/sources/authorities.md#src-36a45a4230) — [riverhog/src/riverhog\_core/throughput.py::\_env\_int](../../../../../../riverhog/src/riverhog_core/throughput.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/throughput.py](../../../../../../riverhog/src/riverhog_core/throughput.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/174`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc19d7cc5faf560a00440b9402f2e14538b0278c30f7c8eea9e5d797b2c6bf82 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY",
  "owner": "riverhog-server"
}
```

</details>
