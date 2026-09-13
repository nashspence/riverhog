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
| Field | Shape |
|---|---|
| <a id="s-398c8d4a0c"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-a19e580965"></a>`distribution` | "riverhog-storage-adapter-aws" |
| <a id="s-73818d255e"></a>`module` | "riverhog_storage_adapter_aws" |
| <a id="s-4ad1319636"></a>`name` | "AwsDeepArchiveReadPreparation" |
| <a id="s-fcb574aee7"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation.status](riverhog-storage-adapter-aws-awsdeeparchivereadpreparation-status.md)
- [riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation.prepare](riverhog-storage-adapter-aws-awsdeeparchivereadpreparation-prepare.md)
- [riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation.cleanup](riverhog-storage-adapter-aws-awsdeeparchivereadpreparation-cleanup.md)

## Governing policies

- <a id="pa-e820e14b7f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources.md#src-5059355196) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation`

### Exact owned JSON

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
