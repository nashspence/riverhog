# riverhog_storage_adapter_protocol.StorageAdapterPort.begin_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-028a5bca5f:314dc53277 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0a5a345fc9"></a>
- <a id="s-613c366e89"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-5e52b0d4ec"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-a42544c822"></a>`name`: `begin_write`
- <a id="s-62ea44bac4"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-8790757a1e"></a>`unit`: `member`

### Declared structure

- <a id="s-249215a285"></a>`kind`: `"method"`
- <a id="s-1b49cc8b5d"></a>`signature`: `"\"(self, request: 'WriteStartRequest') -> 'WriteSession'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-68be9dba1a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.begin_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d5b5162e62f017636d002bc21983168d6d11c66994825ac2469ad669088b0109 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteStartRequest') -> 'WriteSession'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "begin_write",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```

</details>
