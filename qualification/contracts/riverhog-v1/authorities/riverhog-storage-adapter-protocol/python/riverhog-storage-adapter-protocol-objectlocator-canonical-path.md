# riverhog_storage_adapter_protocol.ObjectLocator.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectl-5297289b14:b196a0b34f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be19e76a80"></a>
- <a id="s-518af8e4d9"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-f3f9c089f7"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-831c49ec04"></a>`name`: `canonical_path`
- <a id="s-a47546269b"></a>`owner`: `riverhog_storage_adapter_protocol.ObjectLocator`
- <a id="s-7040340e30"></a>`unit`: `member`

### Declared structure

- <a id="s-aa91ffeb18"></a>`kind`: `"classmethod"`
- <a id="s-6ef7e17fde"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ObjectLocator](riverhog-storage-adapter-protocol-objectlocator.md)

## Governing policies

- <a id="pa-d86ba96d5c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectLocator.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 443971f8350e1323eabc78eeadb48955497c6bd74932be30f18d352632d938ac -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_path",
  "owner": "riverhog_storage_adapter_protocol.ObjectLocator",
  "unit": "member"
}
```

</details>
