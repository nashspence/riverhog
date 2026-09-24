# stove0_protocol.CollectionRootIdentityRef.from_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-collectionrootidentityref-2ae621ad02:9855cda3bf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a8b00664e4"></a>
- <a id="s-93934da4fa"></a>`distribution`: `stove0-protocol`
- <a id="s-36f1202a81"></a>`module`: `stove0_protocol`
- <a id="s-e9ab3036ad"></a>`name`: `from_identity`
- <a id="s-bc53f8d847"></a>`owner`: `stove0_protocol.CollectionRootIdentityRef`
- <a id="s-cbd3a8985a"></a>`unit`: `member`

### Declared structure

- <a id="s-0be5d100c1"></a>`kind`: `"classmethod"`
- <a id="s-5e6b76d673"></a>`signature`: `"\"(cls, value: 'CollectionRootIdentity') -> 'CollectionRootIdentityRef'\""`

## Maintained corroboration

### Related interface records

- [CollectionRootIdentityRef](stove0-protocol-collectionrootidentityref.md)

## Governing policies

- <a id="pa-330cd32d75"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.CollectionRootIdentityRef.from_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 10a6f3b7b438bf5107ee99cb1079f27d7d9620a47b8ee1a9bf8c4d60a1203b85 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'CollectionRootIdentity') -> 'CollectionRootIdentityRef'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_identity",
  "owner": "stove0_protocol.CollectionRootIdentityRef",
  "unit": "member"
}
```

</details>
