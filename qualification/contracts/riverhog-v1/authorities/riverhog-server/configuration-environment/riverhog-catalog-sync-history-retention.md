# RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-history-retention:183016240f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-af76c67541"></a>

| Field | Value |
|---|---|
| <a id="s-bed40d38df"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-66ce145b7d"></a>`default_expressions` | `["'30d'"]` |
| <a id="s-ccca75dedb"></a>`id` | `"riverhog-server:environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION"` |
| <a id="s-3206dad185"></a>`input_shape` | `"environment-string"` |
| <a id="s-5dc06a10d9"></a>`name` | `"RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION"` |
| <a id="s-e81be50e39"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](#s-af76c67541) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e4e62e7a8d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-e8490cf655"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](../../../evidence/sources/authorities.md#src-735831a8d4) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION', '30d')` |

### Machine authority

- `/external_contract/configuration_environment/188`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb22db700f94d6046014e9211347cf95e9d649e757d5ad433e37bd2bbf701ba4 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'30d'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION",
  "input_shape": "environment-string",
  "name": "RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION",
  "owner": "riverhog-server"
}
```

</details>
