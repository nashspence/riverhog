# riverhog_storage_adapter_protocol.ObjectReadStream

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectreadstream:60ce198254 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-362d505ea3"></a>
- <a id="s-c2978c1fd4"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-eb1df57fb0"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-2da35c1c10"></a>`name`: `ObjectReadStream`
- <a id="s-6d4ec53de4"></a>`unit`: `export`

### Declared structure

- <a id="s-c5988b9ad6"></a>`kind`: `"class"`
- <a id="s-baa29335b4"></a>`signature`: `"\"(*, receipt: 'ObjectReadReceipt', content: 'Iterator[bytes]', close: 'Callable[[], None] \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [__exit__](riverhog-storage-adapter-protocol-objectreadstream-exit.md)
- [__enter__](riverhog-storage-adapter-protocol-objectreadstream-enter.md)
- [close](riverhog-storage-adapter-protocol-objectreadstream-close.md)

## Governing policies

- <a id="pa-fcce64ca17"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectReadStream`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a0d6539f58357e13833bbd7a0fa3eead382f9d430306a4e8a21e428423b33d4 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, receipt: 'ObjectReadReceipt', content: 'Iterator[bytes]', close: 'Callable[[], None] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectReadStream",
  "unit": "export"
}
```
