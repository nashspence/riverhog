# RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-event-context-reap-batch-size:e2d5da10a2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-787fd4b453"></a>

| Field | Value |
|---|---|
| <a id="s-5f53685028"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-9f8a9d0439"></a>`default_expressions` | `["'100'"]` |
| <a id="s-8cd9a2b240"></a>`id` | `"riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE"` |
| <a id="s-46621afb01"></a>`input_shape` | `"environment-string"` |
| <a id="s-18c3d1d37b"></a>`name` | `"RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE"` |
| <a id="s-7047319589"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](#s-787fd4b453) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-148b8ccb1e"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-312669fece"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../../../evidence/sources/authorities.md#src-fef401b6a9) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE', '100')` |

### Machine authority

- `/external_contract/configuration_environment/192`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7cdef7895f4357311bec98c50f6d4092a7dd4220557df1902e613121a9feca77 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'100'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE",
  "owner": "riverhog-server"
}
```

</details>
