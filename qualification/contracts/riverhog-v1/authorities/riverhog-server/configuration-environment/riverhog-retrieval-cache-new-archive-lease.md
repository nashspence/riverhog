# RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-new-archive-lease:41a0f34b49 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-eb8aec9d18"></a>

| Field | Value |
|---|---|
| <a id="s-0a87e6a26d"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-e548f6ba22"></a>`default_expressions` | `["'72h'"]` |
| <a id="s-919bb7b7ae"></a>`id` | `"riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"` |
| <a id="s-ef9ddf40ef"></a>`input_shape` | `"environment-string"` |
| <a id="s-1288e3cab6"></a>`name` | `"RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"` |
| <a id="s-5aacd24817"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](#s-eb8aec9d18) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-daf97f098a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-cd2b3dc8f7"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../../../evidence/sources/authorities.md#src-22c1f9d04c) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE', '72h')` |

### Machine authority

- `/external_contract/configuration_environment/204`

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
