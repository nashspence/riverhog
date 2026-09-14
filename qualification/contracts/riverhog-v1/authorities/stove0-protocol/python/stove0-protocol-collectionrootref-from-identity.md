# stove0_protocol.CollectionRootRef.from_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-collectionrootref-from-identity:540a25950e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d14aecaf3"></a>
- <a id="s-32b3cc80a2"></a>`distribution`: `stove0-protocol`
- <a id="s-766dd9625a"></a>`module`: `stove0_protocol`
- <a id="s-b8593b4b59"></a>`name`: `from_identity`
- <a id="s-aca5137e54"></a>`owner`: `stove0_protocol.CollectionRootRef`
- <a id="s-601afaaf55"></a>`unit`: `member`

### Declared structure

- <a id="s-11bc087668"></a>`kind`: `"classmethod"`
- <a id="s-9e9e7f2f64"></a>`signature`: `"\"(cls, value: 'CollectionRootIdentity') -> 'CollectionRootRef'\""`

## Maintained corroboration

### Related interface records

- [CollectionRootRef](stove0-protocol-collectionrootref.md)

## Governing policies

- <a id="pa-1624ecb775"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CollectionRootRef.from_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5226cd3952c0128cc0b2a325c2ee2b847ed5b3b33bf2798ba1f5f8776e774d1 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'CollectionRootIdentity') -> 'CollectionRootRef'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_identity",
  "owner": "stove0_protocol.CollectionRootRef",
  "unit": "member"
}
```
