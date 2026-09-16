# RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-new-archive-lease:1248bb12d3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-91af6fdd5c"></a>

| Field | Value |
|---|---|
| <a id="s-b3f49ca63f"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-915d92d4ad"></a>`default_expressions` | `["'72h'"]` |
| <a id="s-202000a564"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"` |
| <a id="s-7d40f9b692"></a>`input_shape` | `"environment-string"` |
| <a id="s-814b85f667"></a>`name` | `"RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"` |
| <a id="s-4cd44f5a55"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](#s-91af6fdd5c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-19c32b701a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-e4c5602084"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../../../evidence/sources.md#src-22c1f9d04c) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE', '72h')` |

### Machine authority

- `/external_contract/configuration_environment/70`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 208e8cf604682279849a1fc8cb1798124c04a1e108f067f35cd9d7176ced97f0 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'72h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE",
  "owner": "riverhog-server"
}
```

</details>
