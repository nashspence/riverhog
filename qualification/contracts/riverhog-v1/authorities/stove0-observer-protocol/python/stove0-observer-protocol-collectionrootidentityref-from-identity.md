# stove0_observer_protocol.CollectionRootIdentityRef.from_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-collectionrootid-8085a1450c:176ca29b05 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6746ed3dab"></a>
- <a id="s-625527beab"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-a3f65415a0"></a>`module`: `stove0_observer_protocol`
- <a id="s-5f71a27865"></a>`name`: `from_identity`
- <a id="s-1de90aa4cc"></a>`owner`: `stove0_observer_protocol.CollectionRootIdentityRef`
- <a id="s-b782df83f4"></a>`unit`: `member`

### Declared structure

- <a id="s-f0a2d54a14"></a>`kind`: `"classmethod"`
- <a id="s-bb59215bb5"></a>`signature`: `"\"(cls, value: 'CollectionRootIdentity') -> 'CollectionRootIdentityRef'\""`

## Maintained corroboration

### Related interface records

- [CollectionRootIdentityRef](stove0-observer-protocol-collectionrootidentityref.md)

## Governing policies

- <a id="pa-b54c77754a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.CollectionRootIdentityRef.from_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7fff2406f3d93a5d76253c0dd637b905a471d76ded356c6c3ff506e42e6cd6a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'CollectionRootIdentity') -> 'CollectionRootIdentityRef'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "from_identity",
  "owner": "stove0_observer_protocol.CollectionRootIdentityRef",
  "unit": "member"
}
```

</details>
