# riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_completed_at

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-complet-0dd08b52ae:45a64046a3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2cdc843e30"></a>
- <a id="s-c82b70e58b"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-6816785975"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-8cdbd6f8a0"></a>`name`: `canonical_completed_at`
- <a id="s-f14f06795c"></a>`owner`: `riverhog_storage_adapter_protocol.CompletedObjectReceipt`
- <a id="s-e4b6142c0c"></a>`unit`: `member`

### Declared structure

- <a id="s-3d83d34e73"></a>`kind`: `"classmethod"`
- <a id="s-736343c1a0"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [CompletedObjectReceipt](riverhog-storage-adapter-protocol-completedobjectreceipt.md)

## Governing policies

- <a id="pa-561bb05567"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_completed_at`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5648b1cb2ba5b138790a5d2aaae982ae04b088f41176ea13a25993ff1a3c0460 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_completed_at",
  "owner": "riverhog_storage_adapter_protocol.CompletedObjectReceipt",
  "unit": "member"
}
```

</details>
