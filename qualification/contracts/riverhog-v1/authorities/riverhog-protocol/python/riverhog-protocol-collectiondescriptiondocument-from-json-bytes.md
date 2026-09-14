# riverhog_protocol.CollectionDescriptionDocument.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiondescriptiondo-dffd5ef526:3c5d36d17a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7515ea618f"></a>
- <a id="s-01f1ff10e3"></a>`distribution`: `riverhog-protocol`
- <a id="s-d4ba21ceb6"></a>`module`: `riverhog_protocol`
- <a id="s-50aef696df"></a>`name`: `from_json_bytes`
- <a id="s-20a755c4a0"></a>`owner`: `riverhog_protocol.CollectionDescriptionDocument`
- <a id="s-94d8d20eee"></a>`unit`: `member`

### Declared structure

- <a id="s-85bdee094c"></a>`kind`: `"classmethod"`
- <a id="s-ff993fa9e8"></a>`signature`: `"\"(cls, content: 'bytes \| str') -> 'CollectionDescriptionDocument'\""`

## Maintained corroboration

### Related interface records

- [CollectionDescriptionDocument](riverhog-protocol-collectiondescriptiondocument.md)

## Governing policies

- <a id="pa-8751ccaba9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDescriptionDocument.from_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22b62834f9ab9430045d92439908a58a74742a2c70b71fd5b80a94c9203d0994 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, content: 'bytes | str') -> 'CollectionDescriptionDocument'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_json_bytes",
  "owner": "riverhog_protocol.CollectionDescriptionDocument",
  "unit": "member"
}
```
