# RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-history-reap-batch-size:cb32108da6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-82450fb00f"></a>

| Field | Value |
|---|---|
| <a id="s-d73c536259"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-dc224ebacb"></a>`default_expressions` | `["'100'"]` |
| <a id="s-16d235080a"></a>`id` | `"riverhog-server:environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE"` |
| <a id="s-a9b457de03"></a>`input_shape` | `"environment-string"` |
| <a id="s-143d7ec0d5"></a>`name` | `"RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE"` |
| <a id="s-be5f57ae50"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](#s-82450fb00f) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-7292535ecc"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-819bf98f80"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../../../evidence/sources/authorities.md#src-e04d565c18) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE', '100')` |

### Machine authority

- `/external_contract/configuration_environment/187`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5630af3c50bdbd0b6b85dec0f1322237fe735de53a1bc7e1b2e83bd360da6cab -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'100'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE",
  "owner": "riverhog-server"
}
```

</details>
