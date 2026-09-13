# riverhog_storage_adapter_aws.AwsCloudFrontObjectReader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-awscloudfron-b040c673dc:44a0e3f04b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1360ed7a0f"></a>
| Field | Shape |
|---|---|
| <a id="s-7932f93f74"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ce1d91859e"></a>`distribution` | "riverhog-storage-adapter-aws" |
| <a id="s-d49a248e6f"></a>`module` | "riverhog_storage_adapter_aws" |
| <a id="s-50f65a3358"></a>`name` | "AwsCloudFrontObjectReader" |
| <a id="s-d1e73da5f0"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_aws.AwsCloudFrontObjectReader.close](riverhog-storage-adapter-aws-awscloudfrontobjectreader-close.md)
- [riverhog_storage_adapter_aws.AwsCloudFrontObjectReader.read_object](riverhog-storage-adapter-aws-awscloudfrontobjectreader-read-object.md)

## Governing policies

- <a id="pa-878fd90fb8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources.md#src-5059355196) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsCloudFrontObjectReader`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5eb2f690dab37c088efcb407cdb2426b3f7e51485871213a1b1600e7ff4bed1d -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(config: 'AwsCloudFrontConfig', *, client: 'httpx.Client | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-aws",
  "module": "riverhog_storage_adapter_aws",
  "name": "AwsCloudFrontObjectReader",
  "unit": "export"
}
```
