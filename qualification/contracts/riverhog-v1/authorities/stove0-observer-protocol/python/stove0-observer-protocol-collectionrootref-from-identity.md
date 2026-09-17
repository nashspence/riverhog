# stove0_observer_protocol.CollectionRootRef.from_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-collectionrootre-acb59f95cc:b1359d4fe4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ca4c9ede9a"></a>
- <a id="s-364b6f9ce1"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-4182981ec0"></a>`module`: `stove0_observer_protocol`
- <a id="s-f648e63a97"></a>`name`: `from_identity`
- <a id="s-6c58cd36dc"></a>`owner`: `stove0_observer_protocol.CollectionRootRef`
- <a id="s-ec313b7805"></a>`unit`: `member`

### Declared structure

- <a id="s-c88ad7d97c"></a>`kind`: `"classmethod"`
- <a id="s-de91a71499"></a>`signature`: `"\"(cls, value: 'CollectionRootIdentity') -> 'CollectionRootRef'\""`

## Maintained corroboration

### Related interface records

- [CollectionRootRef](stove0-observer-protocol-collectionrootref.md)

## Governing policies

- <a id="pa-d11d68e47e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.CollectionRootRef.from_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d215ffe4a79c4738d6f4c0427f7fa5f80be10253ffbb22f183d832b4ef75ea87 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'CollectionRootIdentity') -> 'CollectionRootRef'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "from_identity",
  "owner": "stove0_observer_protocol.CollectionRootRef",
  "unit": "member"
}
```

</details>
