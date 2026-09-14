# riverhog_protocol.collection_tag_node_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-tag-node-path:a47feabaf3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee3bda8fdb"></a>
- <a id="s-bf05488201"></a>`distribution`: `riverhog-protocol`
- <a id="s-5f255302ed"></a>`module`: `riverhog_protocol`
- <a id="s-e2b69fff54"></a>`name`: `collection_tag_node_path`
- <a id="s-1f252b1aff"></a>`unit`: `export`

### Declared structure

- <a id="s-55ea347fa5"></a>`kind`: `"function"`
- <a id="s-e75007fb0f"></a>`signature`: `"\"(digest: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-2c87ae067a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_tag_node_path`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c008623146df4e60d8b19be5bee4edcb11f0cfab671a7e11a475eb69fd72a68a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(digest: 'str') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_tag_node_path",
  "unit": "export"
}
```
