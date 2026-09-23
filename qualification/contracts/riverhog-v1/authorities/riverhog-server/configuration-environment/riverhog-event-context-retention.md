# RIVERHOG_EVENT_CONTEXT_RETENTION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-event-context-retention:ea61697682 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-749bd6e141"></a>

| Field | Value |
|---|---|
| <a id="s-032458916b"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-595d4c8a28"></a>`default_expressions` | `["'30d'"]` |
| <a id="s-6b131cbb88"></a>`id` | `"riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_RETENTION"` |
| <a id="s-deda87c07c"></a>`input_shape` | `"environment-string"` |
| <a id="s-94ad8621b7"></a>`name` | `"RIVERHOG_EVENT_CONTEXT_RETENTION"` |
| <a id="s-c4e479ca84"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_EVENT_CONTEXT_RETENTION"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_EVENT_CONTEXT_RETENTION](#s-749bd6e141) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8791769d99"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-618887a499"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_RETENTION](../../../evidence/sources/authorities.md#src-42756a2b0a) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_EVENT_CONTEXT_RETENTION', '30d')` |

### Machine authority

- `/external_contract/configuration_environment/59`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8ee8066b84c86abf02032e8479f63046c4b04299e199405450744ab45d701ba -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'30d'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_EVENT_CONTEXT_RETENTION",
  "input_shape": "environment-string",
  "name": "RIVERHOG_EVENT_CONTEXT_RETENTION",
  "owner": "riverhog-server"
}
```

</details>
