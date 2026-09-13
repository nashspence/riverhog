# riverhog_protocol.CollectionTagNode

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagnode:37149abd5b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-828ade08b3"></a>
| Field | Shape |
|---|---|
| <a id="s-5d94f2d02f"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-b66beb27fc"></a>`distribution` | "riverhog-protocol" |
| <a id="s-73ccb36f87"></a>`module` | "riverhog_protocol" |
| <a id="s-b757839c0d"></a>`name` | "CollectionTagNode" |
| <a id="s-d61d7621cf"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1dbf4a39a0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagNode`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f7f1e271b6ef8122b7076eba66c33b77fa6e01d186a8ddfd5dcb9070e1039ed1 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "prefix",
        "type": "'bytes'"
      },
      {
        "default": "None",
        "name": "tag",
        "type": "'bytes | None'"
      },
      {
        "default": "()",
        "name": "children",
        "type": "'tuple[CollectionTagChild, ...]'"
      }
    ],
    "kind": "class",
    "signature": "\"(prefix: 'bytes', tag: 'bytes | None' = None, children: 'tuple[CollectionTagChild, ...]' = ()) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionTagNode",
  "unit": "export"
}
```
