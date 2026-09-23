# a_riverhog_aws_store.AwsDeepArchiveReadPreparation.cleanup

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-aws-store:a-riverhog-aws-store-awsdeeparchivereadpr-688f48bc4a:03bc80741f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8a47a40b5f"></a>
- <a id="s-7e460105c3"></a>`distribution`: `a-riverhog-aws-store`
- <a id="s-3b4cb72d0e"></a>`module`: `a_riverhog_aws_store`
- <a id="s-2afef01853"></a>`name`: `cleanup`
- <a id="s-3a71b34956"></a>`owner`: `a_riverhog_aws_store.AwsDeepArchiveReadPreparation`
- <a id="s-f2676002b6"></a>`unit`: `member`

### Declared structure

- <a id="s-a090f0c239"></a>`kind`: `"method"`
- <a id="s-4f57a9d85d"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [AwsDeepArchiveReadPreparation](a-riverhog-aws-store-awsdeeparchivereadpreparation.md)

## Governing policies

- <a id="pa-236a16a5cd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-aws-store:a_riverhog_aws_store](../../../evidence/sources/authorities.md#src-41b0089d86) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_aws_store.AwsDeepArchiveReadPreparation.cleanup`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 66893a241f80fb3ec92bd4b4b23777e2f7cc806588a6e8b7bfaf59720a65554c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'None'\""
  },
  "distribution": "a-riverhog-aws-store",
  "module": "a_riverhog_aws_store",
  "name": "cleanup",
  "owner": "a_riverhog_aws_store.AwsDeepArchiveReadPreparation",
  "unit": "member"
}
```

</details>
