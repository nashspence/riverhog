# riverhog_storage_adapter_aws.AwsCloudFrontObjectReader.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-awscloudfron-920a68fd5e:b15d25cd87 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b437add6b1"></a>
| Field | Shape |
|---|---|
| <a id="s-5e553ab253"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4959114743"></a>`distribution` | "riverhog-storage-adapter-aws" |
| <a id="s-e8c5cb87f5"></a>`module` | "riverhog_storage_adapter_aws" |
| <a id="s-1a6acde0d4"></a>`name` | "close" |
| <a id="s-f7321b7d49"></a>`owner` | "riverhog_storage_adapter_aws.AwsCloudFrontObjectReader" |
| <a id="s-242c733803"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_aws.AwsCloudFrontObjectReader](riverhog-storage-adapter-aws-awscloudfrontobjectreader.md)

## Governing policies

- <a id="pa-6c389312be"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources.md#src-5059355196) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsCloudFrontObjectReader.close`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64d2dccccd358f386482843843ce0a1c81fe9265cc7a553142f4596567afbbd8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-aws",
  "module": "riverhog_storage_adapter_aws",
  "name": "close",
  "owner": "riverhog_storage_adapter_aws.AwsCloudFrontObjectReader",
  "unit": "member"
}
```
