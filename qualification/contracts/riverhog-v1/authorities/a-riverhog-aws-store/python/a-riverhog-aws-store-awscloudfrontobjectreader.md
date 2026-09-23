# a_riverhog_aws_store.AwsCloudFrontObjectReader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-aws-store:a-riverhog-aws-store-awscloudfrontobjectreader:5289f3c59b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-24b5feaea8"></a>
- <a id="s-22f1fc5d7b"></a>`distribution`: `a-riverhog-aws-store`
- <a id="s-f09e6e6432"></a>`module`: `a_riverhog_aws_store`
- <a id="s-ed12256439"></a>`name`: `AwsCloudFrontObjectReader`
- <a id="s-67dbd6e71d"></a>`unit`: `export`

### Declared structure

- <a id="s-bd2b4949b9"></a>`kind`: `"class"`
- <a id="s-6e02de91f5"></a>`signature`: `"\"(config: 'AwsCloudFrontConfig', *, client: 'httpx.Client \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [read_object](a-riverhog-aws-store-awscloudfrontobjectreader-read-object.md)
- [close](a-riverhog-aws-store-awscloudfrontobjectreader-close.md)

## Governing policies

- <a id="pa-413514e127"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-aws-store:a_riverhog_aws_store](../../../evidence/sources/authorities.md#src-41b0089d86) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_aws_store.AwsCloudFrontObjectReader`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d2bea6694f9032fe5bb65a0337479d941987982e53a746a0b20e1ea30c17d0f -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(config: 'AwsCloudFrontConfig', *, client: 'httpx.Client | None' = None) -> 'None'\""
  },
  "distribution": "a-riverhog-aws-store",
  "module": "a_riverhog_aws_store",
  "name": "AwsCloudFrontObjectReader",
  "unit": "export"
}
```

</details>
