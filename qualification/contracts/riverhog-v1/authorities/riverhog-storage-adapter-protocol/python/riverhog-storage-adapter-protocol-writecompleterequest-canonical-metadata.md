# riverhog_storage_adapter_protocol.WriteCompleteRequest.canonical_metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writeco-1fe554d1ab:bfbb994077 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b918d23328"></a>
- <a id="s-b93b641dca"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-1c11855eee"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-0a5594209f"></a>`name`: `canonical_metadata`
- <a id="s-e7b8a60e25"></a>`owner`: `riverhog_storage_adapter_protocol.WriteCompleteRequest`
- <a id="s-6c7205415b"></a>`unit`: `member`

### Declared structure

- <a id="s-49afea3095"></a>`kind`: `"classmethod"`
- <a id="s-18ba494402"></a>`signature`: `"\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [WriteCompleteRequest](riverhog-storage-adapter-protocol-writecompleterequest.md)

## Governing policies

- <a id="pa-80780b7657"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteCompleteRequest.canonical_metadata`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dea8104f6306207e69c8b49138eb68f141689c2a346b82f23c514f5084a30e5c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'dict[str, str]') -> 'dict[str, str]'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_metadata",
  "owner": "riverhog_storage_adapter_protocol.WriteCompleteRequest",
  "unit": "member"
}
```

</details>
