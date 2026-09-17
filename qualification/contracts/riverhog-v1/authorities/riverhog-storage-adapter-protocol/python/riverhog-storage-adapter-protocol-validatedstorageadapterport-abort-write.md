# riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.abort_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-396914408d:292d2c6f9b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f71600a2d"></a>
- <a id="s-42f345864c"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-b446702d7d"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-a9e2b04a5d"></a>`name`: `abort_write`
- <a id="s-20dcda44ec"></a>`owner`: `riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort`
- <a id="s-7f9d3e31c3"></a>`unit`: `member`

### Declared structure

- <a id="s-53b0c65426"></a>`kind`: `"method"`
- <a id="s-8f452f3b10"></a>`signature`: `"\"(self, session: 'WriteSession') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ValidatedStorageAdapterPort](riverhog-storage-adapter-protocol-validatedstorageadapterport.md)

## Governing policies

- <a id="pa-e0d974c8ea"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.abort_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ba0f1b4c09d9e94c844b0e5be7a69810243a40f2c19ecef5a58a053f13972fe -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, session: 'WriteSession') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "abort_write",
  "owner": "riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort",
  "unit": "member"
}
```

</details>
