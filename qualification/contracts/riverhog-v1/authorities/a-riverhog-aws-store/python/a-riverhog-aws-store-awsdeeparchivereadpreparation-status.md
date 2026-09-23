# a_riverhog_aws_store.AwsDeepArchiveReadPreparation.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-aws-store:a-riverhog-aws-store-awsdeeparchivereadpr-61e9659b17:7450253e18 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c40c3ace97"></a>
- <a id="s-88cefb4024"></a>`distribution`: `a-riverhog-aws-store`
- <a id="s-e89a4613c4"></a>`module`: `a_riverhog_aws_store`
- <a id="s-9a33e79eed"></a>`name`: `status`
- <a id="s-e5def09a60"></a>`owner`: `a_riverhog_aws_store.AwsDeepArchiveReadPreparation`
- <a id="s-c05449a07b"></a>`unit`: `member`

### Declared structure

- <a id="s-d54d375ca3"></a>`kind`: `"method"`
- <a id="s-e0d7978659"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'ReadReadiness'\""`

## Maintained corroboration

### Related interface records

- [AwsDeepArchiveReadPreparation](a-riverhog-aws-store-awsdeeparchivereadpreparation.md)

## Governing policies

- <a id="pa-ad8db63b6a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-aws-store:a_riverhog_aws_store](../../../evidence/sources/authorities.md#src-41b0089d86) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_aws_store.AwsDeepArchiveReadPreparation.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 381c948118a67b1abab6750bf01af078aa312fb9afe5da61f8e05cbca50c6203 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
  },
  "distribution": "a-riverhog-aws-store",
  "module": "a_riverhog_aws_store",
  "name": "status",
  "owner": "a_riverhog_aws_store.AwsDeepArchiveReadPreparation",
  "unit": "member"
}
```

</details>
