# RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-history-reap-batch-size:8fc04ca547 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1cd6fe0a97"></a>
| Field | Shape |
|---|---|
| <a id="s-923821698e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-31873541f2"></a>`default_expressions` | ["'100'"] |
| <a id="s-a7a2756f2e"></a>`id` | "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE" |
| <a id="s-f7fb27f6db"></a>`input_shape` | "environment-string" |
| <a id="s-8f98162f43"></a>`name` | "RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE" |
| <a id="s-5f637bee10"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](#s-1cd6fe0a97) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-c46d7494be"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-0e8d5aadbc"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../../../evidence/sources.md#src-e04d565c18) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE', '100')` |

### Machine authority

- `/external_contract/configuration_environment/53`

### Exact owned JSON

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
