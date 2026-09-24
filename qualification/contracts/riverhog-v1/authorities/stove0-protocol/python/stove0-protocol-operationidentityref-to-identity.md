# stove0_protocol.OperationIdentityRef.to_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-operationidentityref-to-identity:c2695237e1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db5fc063ea"></a>
- <a id="s-75784df6e1"></a>`distribution`: `stove0-protocol`
- <a id="s-50d63d1273"></a>`module`: `stove0_protocol`
- <a id="s-f175605321"></a>`name`: `to_identity`
- <a id="s-80ae32a445"></a>`owner`: `stove0_protocol.OperationIdentityRef`
- <a id="s-c799559acf"></a>`unit`: `member`

### Declared structure

- <a id="s-15b3f27cfc"></a>`kind`: `"method"`
- <a id="s-73da75a96a"></a>`signature`: `"\"(self) -> 'OperationIdentity'\""`

## Maintained corroboration

### Related interface records

- [OperationIdentityRef](stove0-protocol-operationidentityref.md)

## Governing policies

- <a id="pa-33941d54f2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.OperationIdentityRef.to_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a849bdd0c8a2b4841117b9dbde63d52fdf3c488f8d92a83590385888a891cbc9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'OperationIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "to_identity",
  "owner": "stove0_protocol.OperationIdentityRef",
  "unit": "member"
}
```

</details>
