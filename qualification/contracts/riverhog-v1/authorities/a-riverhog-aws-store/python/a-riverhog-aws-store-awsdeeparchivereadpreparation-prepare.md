# a_riverhog_aws_store.AwsDeepArchiveReadPreparation.prepare

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-aws-store:a-riverhog-aws-store-awsdeeparchivereadpr-4dcf868105:9e380c5aeb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-40f58b2190"></a>
- <a id="s-9dd0f75f33"></a>`distribution`: `a-riverhog-aws-store`
- <a id="s-030e7df0fa"></a>`module`: `a_riverhog_aws_store`
- <a id="s-ce14fbbefe"></a>`name`: `prepare`
- <a id="s-4f9db0a0da"></a>`owner`: `a_riverhog_aws_store.AwsDeepArchiveReadPreparation`
- <a id="s-25f5fa59a1"></a>`unit`: `member`

### Declared structure

- <a id="s-44814e7773"></a>`kind`: `"method"`
- <a id="s-d8a0ec2002"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'ReadReadiness'\""`

## Maintained corroboration

### Related interface records

- [AwsDeepArchiveReadPreparation](a-riverhog-aws-store-awsdeeparchivereadpreparation.md)

## Governing policies

- <a id="pa-cd945f9721"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-aws-store:a_riverhog_aws_store](../../../evidence/sources/authorities.md#src-41b0089d86) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_aws_store.AwsDeepArchiveReadPreparation.prepare`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a26dbfde81efc98de20c7fb730df5731c5f131f72115036251808cc72c1fc41 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
  },
  "distribution": "a-riverhog-aws-store",
  "module": "a_riverhog_aws_store",
  "name": "prepare",
  "owner": "a_riverhog_aws_store.AwsDeepArchiveReadPreparation",
  "unit": "member"
}
```

</details>
