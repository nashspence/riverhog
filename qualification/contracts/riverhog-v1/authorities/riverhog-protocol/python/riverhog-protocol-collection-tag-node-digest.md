# riverhog_protocol.collection_tag_node_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-tag-node-digest:5d03679aa1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a6d09ce8d0"></a>
- <a id="s-c5de507ae4"></a>`distribution`: `riverhog-protocol`
- <a id="s-0ed32d12be"></a>`module`: `riverhog_protocol`
- <a id="s-d1db553a8c"></a>`name`: `collection_tag_node_digest`
- <a id="s-4d98c2ec5c"></a>`unit`: `export`

### Declared structure

- <a id="s-9db87abe2e"></a>`kind`: `"function"`
- <a id="s-ea53c0036b"></a>`signature`: `"\"(encoded: 'bytes') -> 'str'\""`

## Governing policies

- <a id="pa-afa1b44bae"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_tag_node_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1ba5b52b8bf4722aab88105449c664dbb3870dbe810b3762aa9adf22851bb46 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(encoded: 'bytes') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_tag_node_digest",
  "unit": "export"
}
```

</details>
