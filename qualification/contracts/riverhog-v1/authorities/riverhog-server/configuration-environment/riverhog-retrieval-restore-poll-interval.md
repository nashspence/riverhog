# RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-restore-poll-interval:b3c8c139ee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2a3f124dad"></a>

| Field | Value |
|---|---|
| <a id="s-4b39157865"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-97b263b2b9"></a>`default_expressions` | `["'5m'"]` |
| <a id="s-c556f2f26b"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL"` |
| <a id="s-577a558dba"></a>`input_shape` | `"environment-string"` |
| <a id="s-5a42b163c0"></a>`name` | `"RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL"` |
| <a id="s-c60ee59f23"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](#s-2a3f124dad) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3f4dcbee0f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-2632394c1e"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../../../evidence/sources/authorities.md#src-4c5aea570f) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL', '5m')` |

### Machine authority

- `/external_contract/configuration_environment/217`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 62fe65df21bec409d33370d2eb0d269950ccd0149699dfd926c27c23521ff7a9 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'5m'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL",
  "owner": "riverhog-server"
}
```

</details>
