# riverhog_storage_adapter_protocol.ReadRequested.canonical_estimated_ready_at

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readreq-7d0a23be42:ef6dc32226 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df221fd3db"></a>
- <a id="s-18b4a16c37"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-43950663c3"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-195731e970"></a>`name`: `canonical_estimated_ready_at`
- <a id="s-68e7a8a6dd"></a>`owner`: `riverhog_storage_adapter_protocol.ReadRequested`
- <a id="s-d85688782b"></a>`unit`: `member`

### Declared structure

- <a id="s-efb43a9b57"></a>`kind`: `"classmethod"`
- <a id="s-7b86e33b15"></a>`signature`: `"\"(cls, value: 'str \| None') -> 'str \| None'\""`

## Maintained corroboration

### Related interface records

- [ReadRequested](riverhog-storage-adapter-protocol-readrequested.md)

## Governing policies

- <a id="pa-bcef2c761e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadRequested.canonical_estimated_ready_at`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5fe763c2eca81c621efdd6c541739ed05d56bade41d526b4315ddc78e3081fd4 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str | None') -> 'str | None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_estimated_ready_at",
  "owner": "riverhog_storage_adapter_protocol.ReadRequested",
  "unit": "member"
}
```

</details>
