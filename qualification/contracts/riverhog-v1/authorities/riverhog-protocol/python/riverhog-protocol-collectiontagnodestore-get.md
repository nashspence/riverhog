# riverhog_protocol.CollectionTagNodeStore.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagnodestore-get:8848ce4ec9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ec7910a11"></a>
- <a id="s-06d6fcc8f9"></a>`distribution`: `riverhog-protocol`
- <a id="s-51f6ac3097"></a>`module`: `riverhog_protocol`
- <a id="s-02ed2c43b0"></a>`name`: `get`
- <a id="s-104a39499d"></a>`owner`: `riverhog_protocol.CollectionTagNodeStore`
- <a id="s-dc210a6ba1"></a>`unit`: `member`

### Declared structure

- <a id="s-618769648f"></a>`kind`: `"method"`
- <a id="s-e47c52c2e0"></a>`signature`: `"\"(self, digest: 'str') -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [CollectionTagNodeStore](riverhog-protocol-collectiontagnodestore.md)

## Governing policies

- <a id="pa-01617fee35"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagNodeStore.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22f1c9a060a5f6a4a29f8e0d9cf829a3d19086be50cad7e7ec7248433793da61 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, digest: 'str') -> 'bytes'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.CollectionTagNodeStore",
  "unit": "member"
}
```

</details>
