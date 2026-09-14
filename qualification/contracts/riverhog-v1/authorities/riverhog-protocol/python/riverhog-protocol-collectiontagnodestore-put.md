# riverhog_protocol.CollectionTagNodeStore.put

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagnodestore-put:b11c3d43ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-29fa7e4e53"></a>
- <a id="s-4c03688f9b"></a>`distribution`: `riverhog-protocol`
- <a id="s-5a2957041a"></a>`module`: `riverhog_protocol`
- <a id="s-3ca5ae5c75"></a>`name`: `put`
- <a id="s-462fe394ce"></a>`owner`: `riverhog_protocol.CollectionTagNodeStore`
- <a id="s-74037abe69"></a>`unit`: `member`

### Declared structure

- <a id="s-5df91c3820"></a>`kind`: `"method"`
- <a id="s-99d50c3b7e"></a>`signature`: `"\"(self, digest: 'str', encoded: 'bytes') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [CollectionTagNodeStore](riverhog-protocol-collectiontagnodestore.md)

## Governing policies

- <a id="pa-e5d8eb017e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagNodeStore.put`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef6373521c7745863ee20cb1ac675286c4e3bcfff0ef4d013c8dedabcfb20ed1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, digest: 'str', encoded: 'bytes') -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "put",
  "owner": "riverhog_protocol.CollectionTagNodeStore",
  "unit": "member"
}
```
