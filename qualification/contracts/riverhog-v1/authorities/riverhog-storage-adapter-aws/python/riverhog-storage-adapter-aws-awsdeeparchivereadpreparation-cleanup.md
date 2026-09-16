# riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation.cleanup

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-awsdeeparchi-e17a1327cf:f25c29ede7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-58d78cbe7e"></a>
- <a id="s-819eed4349"></a>`distribution`: `riverhog-storage-adapter-aws`
- <a id="s-80e9a52b6b"></a>`module`: `riverhog_storage_adapter_aws`
- <a id="s-6a0982dfa2"></a>`name`: `cleanup`
- <a id="s-ffe6a6e19f"></a>`owner`: `riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation`
- <a id="s-9c779d52cd"></a>`unit`: `member`

### Declared structure

- <a id="s-a449cacb5c"></a>`kind`: `"method"`
- <a id="s-0083c55506"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [AwsDeepArchiveReadPreparation](riverhog-storage-adapter-aws-awsdeeparchivereadpreparation.md)

## Governing policies

- <a id="pa-744899321a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources.md#src-5059355196) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation.cleanup`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d8b90a050cb2ae859f1e2fe7a1c7ac8310675b78859004d5cfb1aafd9e417af -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-aws",
  "module": "riverhog_storage_adapter_aws",
  "name": "cleanup",
  "owner": "riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation",
  "unit": "member"
}
```

</details>
