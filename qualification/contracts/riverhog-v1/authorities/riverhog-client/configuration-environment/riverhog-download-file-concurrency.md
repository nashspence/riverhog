# RIVERHOG_DOWNLOAD_FILE_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-download-file-concurrency:129e47e37a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-12ac638176"></a>

| Field | Value |
|---|---|
| <a id="s-f82bf50d9b"></a>`consumers` | `["riverhog-client"]` |
| <a id="s-e0ebdca2c1"></a>`default_expressions` | `["''"]` |
| <a id="s-c69a344ed6"></a>`id` | `"riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"` |
| <a id="s-bc447ea0a9"></a>`input_shape` | `"environment-string"` |
| <a id="s-b8a774d9b2"></a>`name` | `"RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"` |
| <a id="s-20b9a9d8a6"></a>`owner` | `"riverhog-client"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_FILE_CONCURRENCY"; consumers=["riverhog-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](#s-12ac638176) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-a58493261c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-965dcbdccb"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../../../evidence/sources.md#src-bb20193981) — [packages/riverhog-client/src/riverhog\_client/downloads.py::configured\_download\_concurrency](../../../../../../packages/riverhog-client/src/riverhog_client/downloads.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-client` | [packages/riverhog-client/src/riverhog\_client/downloads.py](../../../../../../packages/riverhog-client/src/riverhog_client/downloads.py) | `environment.get('RIVERHOG_DOWNLOAD_FILE_CONCURRENCY', '')` |

### Machine authority

- `/external_contract/configuration_environment/14`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2f649f5e7447801198a948df67f2ccc53d6fd7234e8e8e5f35cd0c6f73260ff -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-client:environment:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_DOWNLOAD_FILE_CONCURRENCY",
  "owner": "riverhog-client"
}
```

</details>
