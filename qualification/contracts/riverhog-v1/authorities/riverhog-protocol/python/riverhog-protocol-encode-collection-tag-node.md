# riverhog_protocol.encode_collection_tag_node

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-encode-collection-tag-node:2b811150d2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0e731a658f"></a>
- <a id="s-70fc7887c6"></a>`distribution`: `riverhog-protocol`
- <a id="s-feeb52bb10"></a>`module`: `riverhog_protocol`
- <a id="s-ad33b74a42"></a>`name`: `encode_collection_tag_node`
- <a id="s-31cfbf55b0"></a>`unit`: `export`

### Declared structure

- <a id="s-421c6b90f5"></a>`kind`: `"function"`
- <a id="s-945c968806"></a>`signature`: `"\"(node: 'CollectionTagNode') -> 'bytes'\""`

## Governing policies

- <a id="pa-ca44270a8a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.encode_collection_tag_node`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c281ec55e1bad84f2b83e38b7b87274c527fdf82497e9789a17fad3cc188487 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(node: 'CollectionTagNode') -> 'bytes'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "encode_collection_tag_node",
  "unit": "export"
}
```

</details>
