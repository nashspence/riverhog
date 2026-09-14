# riverhog_protocol.collection_upload_path_order_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-upload-path-order-key:0840ac9e8a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df40d1d046"></a>
- <a id="s-ed6e2cbfce"></a>`distribution`: `riverhog-protocol`
- <a id="s-c66f5098b0"></a>`module`: `riverhog_protocol`
- <a id="s-143736da07"></a>`name`: `collection_upload_path_order_key`
- <a id="s-25f5b56765"></a>`unit`: `export`

### Declared structure

- <a id="s-b262cf94b0"></a>`kind`: `"function"`
- <a id="s-0d9ea62618"></a>`signature`: `"\"(path: 'str') -> 'tuple[int, bytes]'\""`

## Governing policies

- <a id="pa-8aecafc233"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_upload_path_order_key`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc8c38f301ccc3c55188c04957bbc89e1dc94c6a7d1f8ae8b639355b249737ce -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'str') -> 'tuple[int, bytes]'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_upload_path_order_key",
  "unit": "export"
}
```
