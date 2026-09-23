# RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-catalog-sync-page-size-max:58e090f37d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d930fc1ede"></a>

| Field | Value |
|---|---|
| <a id="s-31a8a30674"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-bb8684b48d"></a>`default_expressions` | `["str(CATALOG_SYNC_PAGE_SIZE_MAX)"]` |
| <a id="s-ddfe0f3dd5"></a>`id` | `"riverhog-server:environment:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"` |
| <a id="s-7e37cbb2be"></a>`input_shape` | `"environment-string"` |
| <a id="s-cb77dd8dad"></a>`name` | `"RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"` |
| <a id="s-0393167173"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](#s-d930fc1ede) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2193f9b77f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-6bb5f41361"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../../../evidence/sources/authorities.md#src-65433124fb) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX', str(CATALOG_SYNC_PAGE_SIZE_MAX))` |

### Machine authority

- `/external_contract/configuration_environment/189`

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
