# riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation.prepare

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-awsdeeparchi-36f96defbe:5650dab89c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9336a908c3"></a>
- <a id="s-fb8164f42d"></a>`distribution`: `riverhog-storage-adapter-aws`
- <a id="s-57dcb27e0b"></a>`module`: `riverhog_storage_adapter_aws`
- <a id="s-db2aad83f9"></a>`name`: `prepare`
- <a id="s-e48f16d1c6"></a>`owner`: `riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation`
- <a id="s-cc82197bb4"></a>`unit`: `member`

### Declared structure

- <a id="s-59899e6bda"></a>`kind`: `"method"`
- <a id="s-7ed8a1bc1c"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'ReadReadiness'\""`

## Maintained corroboration

### Related interface records

- [AwsDeepArchiveReadPreparation](riverhog-storage-adapter-aws-awsdeeparchivereadpreparation.md)

## Governing policies

- <a id="pa-b946a42ae6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources.md#src-5059355196) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation.prepare`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f841d4bbe9b84f7c009566d7956bbc628ee9667f3e73485adcd138302bb706de -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
  },
  "distribution": "riverhog-storage-adapter-aws",
  "module": "riverhog_storage_adapter_aws",
  "name": "prepare",
  "owner": "riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation",
  "unit": "member"
}
```

</details>
