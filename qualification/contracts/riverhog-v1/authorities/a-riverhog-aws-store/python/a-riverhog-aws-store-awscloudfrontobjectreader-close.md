# a_riverhog_aws_store.AwsCloudFrontObjectReader.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-aws-store:a-riverhog-aws-store-awscloudfrontobjectreader-close:be51b78053 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c8ebdf07e0"></a>
- <a id="s-34572c223e"></a>`distribution`: `a-riverhog-aws-store`
- <a id="s-8a6ec88f9d"></a>`module`: `a_riverhog_aws_store`
- <a id="s-b225e40482"></a>`name`: `close`
- <a id="s-eee009df01"></a>`owner`: `a_riverhog_aws_store.AwsCloudFrontObjectReader`
- <a id="s-10cb85a833"></a>`unit`: `member`

### Declared structure

- <a id="s-cbeef4a79d"></a>`kind`: `"method"`
- <a id="s-c84700694f"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [AwsCloudFrontObjectReader](a-riverhog-aws-store-awscloudfrontobjectreader.md)

## Governing policies

- <a id="pa-ca29d1dc54"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-aws-store:a_riverhog_aws_store](../../../evidence/sources/authorities.md#src-41b0089d86) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_aws_store.AwsCloudFrontObjectReader.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52b9224efd6a220afcdd0d45cb9cd2619e98b07dccb9741edb2d493718382ec4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "a-riverhog-aws-store",
  "module": "a_riverhog_aws_store",
  "name": "close",
  "owner": "a_riverhog_aws_store.AwsCloudFrontObjectReader",
  "unit": "member"
}
```

</details>
