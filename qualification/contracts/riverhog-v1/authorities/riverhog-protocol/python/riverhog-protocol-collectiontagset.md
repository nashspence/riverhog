# riverhog_protocol.CollectionTagSet

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagset:5f77052388 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-888dbcc716"></a>
- <a id="s-91c6d5ee41"></a>`distribution`: `riverhog-protocol`
- <a id="s-637a480db6"></a>`module`: `riverhog_protocol`
- <a id="s-0c096484e4"></a>`name`: `CollectionTagSet`
- <a id="s-87679e13d0"></a>`unit`: `export`

### Declared structure

- <a id="s-ad54c88ee0"></a>`kind`: `"class"`
- <a id="s-9f3410180f"></a>`signature`: `"\"(store: 'CollectionTagNodeStore', root: 'CollectionTagSetRoot \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [contains](riverhog-protocol-collectiontagset-contains.md)
- [discard](riverhog-protocol-collectiontagset-discard.md)
- [identity](riverhog-protocol-collectiontagset-identity.md)
- [insert](riverhog-protocol-collectiontagset-insert.md)
- [iter_tags](riverhog-protocol-collectiontagset-iter-tags.md)

## Governing policies

- <a id="pa-8914e5658f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagSet`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 99e7cb1f542845c70959c283b7cc94c071daaa9fd478e98f0cfd146458e3dfae -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(store: 'CollectionTagNodeStore', root: 'CollectionTagSetRoot | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionTagSet",
  "unit": "export"
}
```
