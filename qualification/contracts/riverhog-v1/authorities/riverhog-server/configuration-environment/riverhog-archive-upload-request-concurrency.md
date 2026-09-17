# RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-upload-request-concurrency:1ab8cef668 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ee975e99ad"></a>

| Field | Value |
|---|---|
| <a id="s-ca55d6dc58"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-306a5ba67b"></a>`default_expressions` | `["unset"]` |
| <a id="s-777167e457"></a>`id` | `"riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"` |
| <a id="s-faf3e3ddf2"></a>`input_shape` | `"environment-string"` |
| <a id="s-1a8ef76883"></a>`name` | `"RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"` |
| <a id="s-1095e66092"></a>`owner` | `"riverhog-server"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](#s-ee975e99ad) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-e2e4e55abf"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-4d269e552c"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../../../evidence/sources.md#src-b5ab0e168b) — [riverhog/src/riverhog\_core/throughput.py::\_env\_int](../../../../../../riverhog/src/riverhog_core/throughput.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/throughput.py](../../../../../../riverhog/src/riverhog_core/throughput.py) | `values.get(name)` |

### Machine authority

- `/external_contract/configuration_environment/44`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8aec80624985f2648bca262f02dba64578612529f40e3737696b73bd24527414 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-server:environment:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY",
  "owner": "riverhog-server"
}
```

</details>
