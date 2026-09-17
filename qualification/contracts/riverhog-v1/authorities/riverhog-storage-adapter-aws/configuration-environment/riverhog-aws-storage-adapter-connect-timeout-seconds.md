# RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-connect-timeout-seconds:aedea38f04 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-651129acd1"></a>

| Field | Value |
|---|---|
| <a id="s-0a8dd8ad5b"></a>`consumers` | `["riverhog-storage-adapter-aws"]` |
| <a id="s-506bc75a04"></a>`default_expressions` | `["''"]` |
| <a id="s-448ec91883"></a>`id` | `"riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS"` |
| <a id="s-49c9216ceb"></a>`input_shape` | `"environment-string"` |
| <a id="s-b54b400817"></a>`name` | `"RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS"` |
| <a id="s-8ded7f4c58"></a>`owner` | `"riverhog-storage-adapter-aws"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS"; consumers=["riverhog-storage-adapter-aws"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS](#s-651129acd1) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-fe333d9a37"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-9b2fbf09d8"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS](../../../evidence/sources.md#src-be41c0e7eb) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py::\_optional](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/app.py](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/92`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b73d751e55997e7b650cb13231576b214ec400e0c9092d1aa6b15b93614eb392 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS",
  "owner": "riverhog-storage-adapter-aws"
}
```

</details>
