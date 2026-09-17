# riverhog_storage_adapter_aws.AwsCloudFrontObjectReader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-awscloudfron-b040c673dc:44a0e3f04b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1360ed7a0f"></a>
- <a id="s-ce1d91859e"></a>`distribution`: `riverhog-storage-adapter-aws`
- <a id="s-d49a248e6f"></a>`module`: `riverhog_storage_adapter_aws`
- <a id="s-50f65a3358"></a>`name`: `AwsCloudFrontObjectReader`
- <a id="s-d1e73da5f0"></a>`unit`: `export`

### Declared structure

- <a id="s-4b0bd9e755"></a>`kind`: `"class"`
- <a id="s-07abe25a48"></a>`signature`: `"\"(config: 'AwsCloudFrontConfig', *, client: 'httpx.Client \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [close](riverhog-storage-adapter-aws-awscloudfrontobjectreader-close.md)
- [read_object](riverhog-storage-adapter-aws-awscloudfrontobjectreader-read-object.md)

## Governing policies

- <a id="pa-878fd90fb8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources/authorities.md#src-5059355196) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsCloudFrontObjectReader`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
