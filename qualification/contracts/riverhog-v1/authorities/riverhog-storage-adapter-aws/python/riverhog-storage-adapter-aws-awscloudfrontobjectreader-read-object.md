# riverhog_storage_adapter_aws.AwsCloudFrontObjectReader.read_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-awscloudfron-b4267594d9:72fc11838c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-00bc1c85ad"></a>
- <a id="s-c91137f24f"></a>`distribution`: `riverhog-storage-adapter-aws`
- <a id="s-7535d54b02"></a>`module`: `riverhog_storage_adapter_aws`
- <a id="s-8b8c04612d"></a>`name`: `read_object`
- <a id="s-2aa8407d1c"></a>`owner`: `riverhog_storage_adapter_aws.AwsCloudFrontObjectReader`
- <a id="s-52479ceebb"></a>`unit`: `member`

### Declared structure

- <a id="s-21be59ad23"></a>`kind`: `"method"`
- <a id="s-6cdebfe842"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', key: 'str', object_path: 'str', revision: 'str \| None', offset: 'int \| None', size: 'int \| None', expected_bytes: 'int', chunk_bytes: 'int') -> 'ObjectReadStream'\""`

## Maintained corroboration

### Related interface records

- [AwsCloudFrontObjectReader](riverhog-storage-adapter-aws-awscloudfrontobjectreader.md)

## Governing policies

- <a id="pa-61dc67fa8b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources.md#src-5059355196) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsCloudFrontObjectReader.read_object`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 933165be607e90f0bf80dc5f1f427d9a395ebfc4a90246d404883e15dfc9da38 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', key: 'str', object_path: 'str', revision: 'str | None', offset: 'int | None', size: 'int | None', expected_bytes: 'int', chunk_bytes: 'int') -> 'ObjectReadStream'\""
  },
  "distribution": "riverhog-storage-adapter-aws",
  "module": "riverhog_storage_adapter_aws",
  "name": "read_object",
  "owner": "riverhog_storage_adapter_aws.AwsCloudFrontObjectReader",
  "unit": "member"
}
```
