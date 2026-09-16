# RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-page-size-max:c01454275a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-021597da78"></a>

| Field | Value |
|---|---|
| <a id="s-de450a9408"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-1d23c0d90d"></a>`default_expressions` | `["str(CATALOG_SYNC_PAGE_SIZE_MAX)"]` |
| <a id="s-7b17b012fe"></a>`id` | `"riverhog-server:environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"` |
| <a id="s-487e8c583c"></a>`input_shape` | `"environment-string"` |
| <a id="s-9d016ce2fa"></a>`name` | `"RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"` |
| <a id="s-1cfe26331e"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](#s-021597da78) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-4305e0741e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-a2d3a4474f"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../../../evidence/sources.md#src-65433124fb) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX', str(CATALOG_SYNC_PAGE_SIZE_MAX))` |

### Machine authority

- `/external_contract/configuration_environment/55`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b36bf389d05f98fb4c878ac424c451e62b86af790ebefb991c7f40a8bfa7372 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "str(CATALOG_SYNC_PAGE_SIZE_MAX)"
  ],
  "id": "riverhog-server:environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX",
  "input_shape": "environment-string",
  "name": "RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX",
  "owner": "riverhog-server"
}
```

</details>
