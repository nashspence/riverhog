# riverhog_storage_adapter_aws.AwsCloudFrontObjectReader.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-awscloudfron-920a68fd5e:b15d25cd87 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b437add6b1"></a>
- <a id="s-4959114743"></a>`distribution`: `riverhog-storage-adapter-aws`
- <a id="s-e8c5cb87f5"></a>`module`: `riverhog_storage_adapter_aws`
- <a id="s-1a6acde0d4"></a>`name`: `close`
- <a id="s-f7321b7d49"></a>`owner`: `riverhog_storage_adapter_aws.AwsCloudFrontObjectReader`
- <a id="s-242c733803"></a>`unit`: `member`

### Declared structure

- <a id="s-ce53989870"></a>`kind`: `"method"`
- <a id="s-10485710a1"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [AwsCloudFrontObjectReader](riverhog-storage-adapter-aws-awscloudfrontobjectreader.md)

## Governing policies

- <a id="pa-6c389312be"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources/authorities.md#src-5059355196) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsCloudFrontObjectReader.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
