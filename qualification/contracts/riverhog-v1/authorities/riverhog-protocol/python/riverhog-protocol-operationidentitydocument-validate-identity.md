# riverhog_protocol.OperationIdentityDocument.validate_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-operationidentitydocume-31c913810a:e626a7665e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67c15777f1"></a>
- <a id="s-b82a8eaf9c"></a>`distribution`: `riverhog-protocol`
- <a id="s-a8f3a46329"></a>`module`: `riverhog_protocol`
- <a id="s-d07d0df02c"></a>`name`: `validate_identity`
- <a id="s-4a520dcf90"></a>`owner`: `riverhog_protocol.OperationIdentityDocument`
- <a id="s-3c7507b1f1"></a>`unit`: `member`

### Declared structure

- <a id="s-a929b9680c"></a>`kind`: `"method"`
- <a id="s-42d628b00d"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [OperationIdentityDocument](riverhog-protocol-operationidentitydocument.md)

## Governing policies

- <a id="pa-01a3fb01b2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.OperationIdentityDocument.validate_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c964f1dbd650e289976639c86907fc435e08138fa880b074f824e1add2bc416d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_identity",
  "owner": "riverhog_protocol.OperationIdentityDocument",
  "unit": "member"
}
```

</details>
