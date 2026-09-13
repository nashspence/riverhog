# RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-aws:riverhog-aws-storage-adapter-read-chunk-bytes:8d5deaf8f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-960cd64e68"></a>
| Field | Shape |
|---|---|
| <a id="s-194d4c0b99"></a>`consumers` | ["riverhog-storage-adapter-aws"] |
| <a id="s-64d0a96fda"></a>`default_expressions` | ["''"] |
| <a id="s-9b65d2f1fb"></a>`id` | "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES" |
| <a id="s-21b113f8ca"></a>`input_shape` | "environment-string" |
| <a id="s-fc06ab9bf9"></a>`name` | "RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES" |
| <a id="s-edda95a7cd"></a>`owner` | "riverhog-storage-adapter-aws" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES"; consumers=["riverhog-storage-adapter-aws"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES](#s-960cd64e68) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-53011eabf6"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-23b05916b8"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES](../../../evidence/sources.md#src-f9cdca0f7d) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-aws` | `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/100`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91ceb7795172c5a8802cde9404da42a44fac5746daa91fdb96f60c7014818e1b -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-aws"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-aws:environment:RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES",
  "owner": "riverhog-storage-adapter-aws"
}
```
