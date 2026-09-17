# riverhog_protocol.CollectionUploadSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadsort:fa6b40dd22 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b087b0a983"></a>
- <a id="s-278e9ec268"></a>`distribution`: `riverhog-protocol`
- <a id="s-4ada481650"></a>`module`: `riverhog_protocol`
- <a id="s-6ce432343b"></a>`name`: `CollectionUploadSort`
- <a id="s-226140dd5c"></a>`unit`: `export`

### Declared structure

- <a id="s-a9ab436019"></a>`kind`: `"type-alias"`
- <a id="s-2965e55fe6"></a>`value`: `"typing.Literal['id', 'created_at', 'state', 'bytes', 'files']"`

## Governing policies

- <a id="pa-8e67f89900"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d86a4b2dce134a1da2d08be7357ae1b353d272422da351fc67ee55681008e21 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['id', 'created_at', 'state', 'bytes', 'files']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadSort",
  "unit": "export"
}
```

</details>
