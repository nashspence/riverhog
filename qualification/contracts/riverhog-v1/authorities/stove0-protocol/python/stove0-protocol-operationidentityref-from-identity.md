# stove0_protocol.OperationIdentityRef.from_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-operationidentityref-from-identity:bde98fb18e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cd9be3f58f"></a>
- <a id="s-2a62558e1d"></a>`distribution`: `stove0-protocol`
- <a id="s-ac109c0df3"></a>`module`: `stove0_protocol`
- <a id="s-aecec63025"></a>`name`: `from_identity`
- <a id="s-03148644ef"></a>`owner`: `stove0_protocol.OperationIdentityRef`
- <a id="s-e8dd605d84"></a>`unit`: `member`

### Declared structure

- <a id="s-e89669dcc6"></a>`kind`: `"classmethod"`
- <a id="s-b9422ac191"></a>`signature`: `"\"(cls, value: 'OperationIdentity') -> 'OperationIdentityRef'\""`

## Maintained corroboration

### Related interface records

- [OperationIdentityRef](stove0-protocol-operationidentityref.md)

## Governing policies

- <a id="pa-02c2415acf"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.OperationIdentityRef.from_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74032ff7a64be54186b7b4269d054bda332e604380de39af26e9bfb9f9172749 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'OperationIdentity') -> 'OperationIdentityRef'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_identity",
  "owner": "stove0_protocol.OperationIdentityRef",
  "unit": "member"
}
```

</details>
