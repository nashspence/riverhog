# riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws-awsdeeparchi-24b60e45e8:a9ec7c5e2e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-aws](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-00a9ee64da"></a>
- <a id="s-78b86ee7db"></a>`distribution`: `riverhog-storage-adapter-aws`
- <a id="s-ffb968da17"></a>`module`: `riverhog_storage_adapter_aws`
- <a id="s-7bd98517bb"></a>`name`: `status`
- <a id="s-9e3499cd4c"></a>`owner`: `riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation`
- <a id="s-b525f4b9d8"></a>`unit`: `member`

### Declared structure

- <a id="s-2668190c81"></a>`kind`: `"method"`
- <a id="s-4b9e1cb17f"></a>`signature`: `"\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str \| None], ...]') -> 'ReadReadiness'\""`

## Maintained corroboration

### Related interface records

- [AwsDeepArchiveReadPreparation](riverhog-storage-adapter-aws-awsdeeparchivereadpreparation.md)

## Governing policies

- <a id="pa-9e542c13c3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../../../evidence/sources.md#src-5059355196) — `reference/riverhog/storage/aws/src/riverhog_storage_adapter_aws/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation.status`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bde54408c226fb1f7a932772e87147d78fe35cbd451b1e295361377a5b57e30f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
  },
  "distribution": "riverhog-storage-adapter-aws",
  "module": "riverhog_storage_adapter_aws",
  "name": "status",
  "owner": "riverhog_storage_adapter_aws.AwsDeepArchiveReadPreparation",
  "unit": "member"
}
```
