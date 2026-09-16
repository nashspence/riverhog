# riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.head_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-55aeb746e8:0586e03d47 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b0bf9c9deb"></a>
- <a id="s-67365b3aae"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-99505eba5b"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-acb2a4b36f"></a>`name`: `head_object`
- <a id="s-59f65e8c80"></a>`owner`: `riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort`
- <a id="s-eee14dedc6"></a>`unit`: `member`

### Declared structure

- <a id="s-e435c80647"></a>`kind`: `"method"`
- <a id="s-8b4a21ddfd"></a>`signature`: `"\"(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt \| None'\""`

## Maintained corroboration

### Related interface records

- [ValidatedStorageAdapterPort](riverhog-storage-adapter-protocol-validatedstorageadapterport.md)

## Governing policies

- <a id="pa-1ae74a0e72"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.head_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1abe4ea41a8a70606898a8b31c5e4f09245b2603184bad4e136a98a9709097df -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt | None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "head_object",
  "owner": "riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort",
  "unit": "member"
}
```

</details>
