# riverhog_protocol.MemoryCollectionTagNodeStore.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-memorycollectiontagnodestore-get:8b33fdb5c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-da1d31a444"></a>
- <a id="s-a64e973f74"></a>`distribution`: `riverhog-protocol`
- <a id="s-1ae7e7241a"></a>`module`: `riverhog_protocol`
- <a id="s-505d536884"></a>`name`: `get`
- <a id="s-1dd0648761"></a>`owner`: `riverhog_protocol.MemoryCollectionTagNodeStore`
- <a id="s-2aa2371b17"></a>`unit`: `member`

### Declared structure

- <a id="s-30d72691cf"></a>`kind`: `"method"`
- <a id="s-ae2a83ec79"></a>`signature`: `"\"(self, digest: 'str') -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [MemoryCollectionTagNodeStore](riverhog-protocol-memorycollectiontagnodestore.md)

## Governing policies

- <a id="pa-ca337f69f0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.MemoryCollectionTagNodeStore.get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5abde8be237db439eac1f4cac217d2d79814012750fd8f5db4cf638e3d8c94d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, digest: 'str') -> 'bytes'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.MemoryCollectionTagNodeStore",
  "unit": "member"
}
```
