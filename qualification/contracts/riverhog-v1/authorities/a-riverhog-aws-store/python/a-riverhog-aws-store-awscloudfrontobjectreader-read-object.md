# a_riverhog_aws_store.AwsCloudFrontObjectReader.read_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-aws-store:a-riverhog-aws-store-awscloudfrontobjectr-bb8fd1df96:cee363b9bc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3e0517681a"></a>
- <a id="s-89bb0b4e8d"></a>`distribution`: `a-riverhog-aws-store`
- <a id="s-15e92e0834"></a>`module`: `a_riverhog_aws_store`
- <a id="s-9e2fa87abf"></a>`name`: `read_object`
- <a id="s-272136c39d"></a>`owner`: `a_riverhog_aws_store.AwsCloudFrontObjectReader`
- <a id="s-b4f298a75e"></a>`unit`: `member`

### Declared structure

- <a id="s-558cab29cb"></a>`kind`: `"method"`
- <a id="s-cb12502623"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', key: 'str', object_path: 'str', revision: 'str \| None', offset: 'int \| None', size: 'int \| None', expected_bytes: 'int', chunk_bytes: 'int') -> 'ObjectReadStream'\""`

## Maintained corroboration

### Related interface records

- [AwsCloudFrontObjectReader](a-riverhog-aws-store-awscloudfrontobjectreader.md)

## Governing policies

- <a id="pa-04ea9091a0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-aws-store:a_riverhog_aws_store](../../../evidence/sources/authorities.md#src-41b0089d86) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_aws_store.AwsCloudFrontObjectReader.read_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d90d34ffbdf6171d97422640a075b5941edab8e85f53c110e3435d544cad301d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', key: 'str', object_path: 'str', revision: 'str | None', offset: 'int | None', size: 'int | None', expected_bytes: 'int', chunk_bytes: 'int') -> 'ObjectReadStream'\""
  },
  "distribution": "a-riverhog-aws-store",
  "module": "a_riverhog_aws_store",
  "name": "read_object",
  "owner": "a_riverhog_aws_store.AwsCloudFrontObjectReader",
  "unit": "member"
}
```

</details>
