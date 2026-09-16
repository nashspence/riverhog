# riverhog_protocol.decode_collection_tag_node

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-decode-collection-tag-node:e22c03a6e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5a45bff18a"></a>
- <a id="s-132c94f701"></a>`distribution`: `riverhog-protocol`
- <a id="s-2b79e43053"></a>`module`: `riverhog_protocol`
- <a id="s-16db466a89"></a>`name`: `decode_collection_tag_node`
- <a id="s-01ab644cf7"></a>`unit`: `export`

### Declared structure

- <a id="s-96522ba96d"></a>`kind`: `"function"`
- <a id="s-9ea81a39e8"></a>`signature`: `"\"(encoded: 'bytes') -> 'CollectionTagNode'\""`

## Governing policies

- <a id="pa-a3d7f00bf7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.decode_collection_tag_node`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 021c64433ffd15137db5ac7c5d3b011231ac42e505a3ff358b320e69c15b3a14 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(encoded: 'bytes') -> 'CollectionTagNode'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "decode_collection_tag_node",
  "unit": "export"
}
```

</details>
