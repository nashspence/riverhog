# riverhog_protocol.CollectionTagSetRoot

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagsetroot:d100e73e77 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9f2fb7c810"></a>
| Field | Shape |
|---|---|
| <a id="s-29bd42c814"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-aabab06ccd"></a>`distribution` | "riverhog-protocol" |
| <a id="s-e3c4489a05"></a>`module` | "riverhog_protocol" |
| <a id="s-a9c84ef31c"></a>`name` | "CollectionTagSetRoot" |
| <a id="s-22a4aca000"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionTagSetRoot.seal](riverhog-protocol-collectiontagsetroot-seal.md)

## Governing policies

- <a id="pa-64ce64481e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagSetRoot`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57365ca48f3dc837fc2247c3ed1d06d3c347dd7f4bf60f990be6eae25a2fe874 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "root_sha256",
        "type": "'str | None'"
      },
      {
        "default": "required",
        "name": "tag_set_identity",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(root_sha256: 'str | None', tag_set_identity: 'str') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionTagSetRoot",
  "unit": "export"
}
```
