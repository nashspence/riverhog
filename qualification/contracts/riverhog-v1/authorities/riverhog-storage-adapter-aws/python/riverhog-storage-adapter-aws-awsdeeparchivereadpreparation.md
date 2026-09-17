# riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-awsdeeparchi-845ef7e2f7:8adb5d5a07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-392a67b1f6"></a>
- <a id="s-a19e580965"></a>`distribution`: `riverhog-storage-adapter-aws`
- <a id="s-73818d255e"></a>`module`: `riverhog_storage_adapter_aws`
- <a id="s-4ad1319636"></a>`name`: `AwsDeepArchiveReadPreparation`
- <a id="s-fcb574aee7"></a>`unit`: `export`

### Declared structure

- <a id="s-fbce24ac71"></a>`kind`: `"class"`
- <a id="s-c1e2d3c5c2"></a>`signature`: `"\"(tier: 'str' = 'Bulk', days: 'int' = 3) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-4fb4d52c9d"></a>`tier` | `'str'` | `'Bulk'` |
| <a id="s-41a95ffa62"></a>`days` | `'int'` | `3` |

## Maintained corroboration

### Related interface records

- [status](riverhog-storage-adapter-aws-awsdeeparchivereadpreparation-status.md)
- [prepare](riverhog-storage-adapter-aws-awsdeeparchivereadpreparation-prepare.md)
- [cleanup](riverhog-storage-adapter-aws-awsdeeparchivereadpreparation-cleanup.md)

## Governing policies

- <a id="pa-e820e14b7f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources.md#src-5059355196) — [reference/riverhog/storage/aws/src/riverhog\_storage\_adapter\_aws/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f390e0d30c99f93d5222b1e1a13ecfb3a0f58de8a29aca868dfa4472145437dd -->

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
  "distribution": "riverhog-storage-adapter-aws",
  "module": "riverhog_storage_adapter_aws",
  "name": "AwsDeepArchiveReadPreparation",
  "unit": "export"
}
```

</details>
