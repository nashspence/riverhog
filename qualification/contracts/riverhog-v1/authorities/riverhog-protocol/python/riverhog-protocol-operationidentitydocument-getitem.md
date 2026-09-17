# riverhog_protocol.OperationIdentityDocument.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-operationidentitydocument-getitem:4d8ec1ce08 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-590d7f9675"></a>
- <a id="s-7b84e18916"></a>`distribution`: `riverhog-protocol`
- <a id="s-08b7346b47"></a>`module`: `riverhog_protocol`
- <a id="s-406d96646f"></a>`name`: `__getitem__`
- <a id="s-a8155c5758"></a>`owner`: `riverhog_protocol.OperationIdentityDocument`
- <a id="s-8739690a89"></a>`unit`: `member`

### Declared structure

- <a id="s-1202c7e6f6"></a>`kind`: `"method"`
- <a id="s-80ff5909a6"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [OperationIdentityDocument](riverhog-protocol-operationidentitydocument.md)

## Governing policies

- <a id="pa-ad3b8cd8f1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.OperationIdentityDocument.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bfac7176aa515b0aac2d72ad326221bd2bfdad8b0870fad3bf29ee33187c38d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "__getitem__",
  "owner": "riverhog_protocol.OperationIdentityDocument",
  "unit": "member"
}
```

</details>
