# riverhog_storage_adapter_protocol.ImmutableObjectReceipt.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-immutab-9ba1371fe5:4b3a024f2a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8cb5805c3a"></a>
- <a id="s-2b166f9301"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-bc4a562fda"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-0429e8c557"></a>`name`: `canonical_path`
- <a id="s-771a176673"></a>`owner`: `riverhog_storage_adapter_protocol.ImmutableObjectReceipt`
- <a id="s-c0537cadd1"></a>`unit`: `member`

### Declared structure

- <a id="s-37ad770b3e"></a>`kind`: `"classmethod"`
- <a id="s-d7fbaaff69"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ImmutableObjectReceipt](riverhog-storage-adapter-protocol-immutableobjectreceipt.md)

## Governing policies

- <a id="pa-1d68834a62"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ImmutableObjectReceipt.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1dd9cacec84d35256fbc5aaff3727730f023dba08fa435418f42e4b41b12ebc -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_path",
  "owner": "riverhog_storage_adapter_protocol.ImmutableObjectReceipt",
  "unit": "member"
}
```

</details>
