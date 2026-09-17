# riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-complet-137f86e42b:f6cf5dd71a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b74239de0"></a>
- <a id="s-d140347ca0"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-d19b0f0406"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-a28a34d426"></a>`name`: `canonical_path`
- <a id="s-35b250dcbd"></a>`owner`: `riverhog_storage_adapter_protocol.CompletedObjectReceipt`
- <a id="s-4cdbd547c3"></a>`unit`: `member`

### Declared structure

- <a id="s-b37b283e3d"></a>`kind`: `"classmethod"`
- <a id="s-d96508966d"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [CompletedObjectReceipt](riverhog-storage-adapter-protocol-completedobjectreceipt.md)

## Governing policies

- <a id="pa-303a135928"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a41bb34583418d62d2f568e244af14eb0a1d8ea1d5138569258f09045b6c5e21 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_path",
  "owner": "riverhog_storage_adapter_protocol.CompletedObjectReceipt",
  "unit": "member"
}
```

</details>
