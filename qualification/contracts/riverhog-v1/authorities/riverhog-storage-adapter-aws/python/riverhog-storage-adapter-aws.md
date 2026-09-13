# riverhog_storage_adapter_aws

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws:a758849f30 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6607988a47"></a>
| Field | Shape |
|---|---|
| <a id="s-e78414c26a"></a>`candidate_id` | "python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws" |
| <a id="s-e4fae6e507"></a>`distribution` | "riverhog-storage-adapter-aws" |
| <a id="s-e45d782a71"></a>`exports` | additional keys=`AwsCloudFrontObjectReader`, `AwsDeepArchiveReadPreparation` |
| <a id="s-40885dcbd7"></a>`module` | "riverhog_storage_adapter_aws" |

## Governing policies

- <a id="pa-ccb6d21af6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources.md#src-5059355196) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py`

### Machine authority

- `/external_contract/python/26`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e427a8448c688b57b11e3b313a476e0b4373841dd5df1f4b796d0ef10796c9cf -->

```json
{
  "candidate_id": "python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws",
  "distribution": "riverhog-storage-adapter-aws",
  "exports": {
    "AwsCloudFrontObjectReader": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "read_object": {
          "kind": "method",
          "signature": "\"(self, *, client: 'Any', bucket: 'str', key: 'str', object_path: 'str', revision: 'str | None', offset: 'int | None', size: 'int | None', expected_bytes: 'int', chunk_bytes: 'int') -> 'ObjectReadStream'\""
        }
      },
      "signature": "\"(config: 'AwsCloudFrontConfig', *, client: 'httpx.Client | None' = None) -> 'None'\""
    },
    "AwsDeepArchiveReadPreparation": {
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
      "members": {
        "cleanup": {
          "kind": "method",
          "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'None'\""
        },
        "prepare": {
          "kind": "method",
          "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
        },
        "status": {
          "kind": "method",
          "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
        }
      },
      "signature": "\"(tier: 'str' = 'Bulk', days: 'int' = 3) -> None\""
    }
  },
  "module": "riverhog_storage_adapter_aws"
}
```
