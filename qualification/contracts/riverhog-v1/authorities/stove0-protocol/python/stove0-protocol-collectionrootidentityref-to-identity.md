# stove0_protocol.CollectionRootIdentityRef.to_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-collectionrootidentityref-9d317d8a26:4aa073bebb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db687205b4"></a>
- <a id="s-e18e0e1af6"></a>`distribution`: `stove0-protocol`
- <a id="s-a15f185cfc"></a>`module`: `stove0_protocol`
- <a id="s-add46d3b7d"></a>`name`: `to_identity`
- <a id="s-105f4948db"></a>`owner`: `stove0_protocol.CollectionRootIdentityRef`
- <a id="s-b454ecfa57"></a>`unit`: `member`

### Declared structure

- <a id="s-0be06e4bf8"></a>`kind`: `"method"`
- <a id="s-2439dad8f4"></a>`signature`: `"\"(self) -> 'CollectionRootIdentity'\""`

## Maintained corroboration

### Related interface records

- [CollectionRootIdentityRef](stove0-protocol-collectionrootidentityref.md)

## Governing policies

- <a id="pa-8b5c23131a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.CollectionRootIdentityRef.to_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9dbff074e72407cd025c961ff18bea0bd4ad8f43985139af9c54fda661485e64 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'CollectionRootIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "to_identity",
  "owner": "stove0_protocol.CollectionRootIdentityRef",
  "unit": "member"
}
```

</details>
