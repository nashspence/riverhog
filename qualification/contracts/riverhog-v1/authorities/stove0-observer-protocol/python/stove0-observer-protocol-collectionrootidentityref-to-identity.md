# stove0_observer_protocol.CollectionRootIdentityRef.to_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-collectionrootid-7276101ee6:950f199fc0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-849900c0d8"></a>
- <a id="s-c5ced41332"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-93cb3dd4a6"></a>`module`: `stove0_observer_protocol`
- <a id="s-3885ca1484"></a>`name`: `to_identity`
- <a id="s-d72502974d"></a>`owner`: `stove0_observer_protocol.CollectionRootIdentityRef`
- <a id="s-2e5cad5691"></a>`unit`: `member`

### Declared structure

- <a id="s-965d4fb6d5"></a>`kind`: `"method"`
- <a id="s-f2e59d3b64"></a>`signature`: `"\"(self) -> 'CollectionRootIdentity'\""`

## Maintained corroboration

### Related interface records

- [CollectionRootIdentityRef](stove0-observer-protocol-collectionrootidentityref.md)

## Governing policies

- <a id="pa-a2e6e89853"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.CollectionRootIdentityRef.to_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84185f01ecbf89aabb3d70b6eb4b2ec3f4cce8481e271d86cc774d632235db83 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'CollectionRootIdentity'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "to_identity",
  "owner": "stove0_observer_protocol.CollectionRootIdentityRef",
  "unit": "member"
}
```

</details>
