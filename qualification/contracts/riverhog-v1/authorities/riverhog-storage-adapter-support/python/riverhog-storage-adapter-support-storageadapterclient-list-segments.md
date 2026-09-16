# riverhog_storage_adapter_support.StorageAdapterClient.list_segments

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-e2fecef8d0:2ec1230217 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2007b05094"></a>
- <a id="s-56b9bbad53"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-8bd4d1042d"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-68a3b63fde"></a>`name`: `list_segments`
- <a id="s-efdcc83a57"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-263fc4ac6d"></a>`unit`: `member`

### Declared structure

- <a id="s-b047229de5"></a>`kind`: `"method"`
- <a id="s-d4b8022731"></a>`signature`: `"\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-66fadbc358"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.list_segments`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2598dde3e375d90140b4924a428ba74352bb362e4e07a76e423dbba2a8172473 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "list_segments",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
