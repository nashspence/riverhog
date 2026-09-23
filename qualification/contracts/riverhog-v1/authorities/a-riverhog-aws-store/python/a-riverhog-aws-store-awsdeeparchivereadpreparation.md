# a_riverhog_aws_store.AwsDeepArchiveReadPreparation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-aws-store:a-riverhog-aws-store-awsdeeparchivereadpreparation:bd5907f7fc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-84b3c35b50"></a>
- <a id="s-d14a76b732"></a>`distribution`: `a-riverhog-aws-store`
- <a id="s-b5748578a3"></a>`module`: `a_riverhog_aws_store`
- <a id="s-f208ec4a85"></a>`name`: `AwsDeepArchiveReadPreparation`
- <a id="s-a01e1ee203"></a>`unit`: `export`

### Declared structure

- <a id="s-97590a80ac"></a>`kind`: `"class"`
- <a id="s-9197c00254"></a>`signature`: `"\"(tier: 'str' = 'Bulk', days: 'int' = 3) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-427eecf8ea"></a>`tier` | `'str'` | `'Bulk'` |
| <a id="s-7d1dd1494b"></a>`days` | `'int'` | `3` |

## Maintained corroboration

### Related interface records

- [prepare](a-riverhog-aws-store-awsdeeparchivereadpreparation-prepare.md)
- [status](a-riverhog-aws-store-awsdeeparchivereadpreparation-status.md)
- [cleanup](a-riverhog-aws-store-awsdeeparchivereadpreparation-cleanup.md)

## Governing policies

- <a id="pa-9f69503078"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-aws-store:a_riverhog_aws_store](../../../evidence/sources/authorities.md#src-41b0089d86) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_aws_store.AwsDeepArchiveReadPreparation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68ae3cf8b3f078dbb64fa9aac099b7c066e713f9a8dd08871b1bce1fc2967578 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "'Bulk'",
        "name": "tier",
        "type": "'str'"
      },
      {
        "default": "3",
        "name": "days",
        "type": "'int'"
      }
    ],
    "kind": "class",
    "signature": "\"(tier: 'str' = 'Bulk', days: 'int' = 3) -> None\""
  },
  "distribution": "a-riverhog-aws-store",
  "module": "a_riverhog_aws_store",
  "name": "AwsDeepArchiveReadPreparation",
  "unit": "export"
}
```

</details>
