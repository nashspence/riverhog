# riverhog_protocol.RetrievalCacheState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalcachestate:73df6c6e4c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-862af1de4c"></a>
- <a id="s-dd5ff7ad41"></a>`distribution`: `riverhog-protocol`
- <a id="s-f1c81e781a"></a>`module`: `riverhog_protocol`
- <a id="s-86aa820d37"></a>`name`: `RetrievalCacheState`
- <a id="s-05bb66c607"></a>`unit`: `export`

### Declared structure

- <a id="s-582e0aa26a"></a>`kind`: `"type-alias"`
- <a id="s-f282c91f83"></a>`value`: `"typing.Literal['ready', 'delete_pending', 'deleting']"`

## Governing policies

- <a id="pa-070f751802"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalCacheState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98bef52c755693ceb921c167bb135537b6dea02d636151d82b41df5a81de46bd -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['ready', 'delete_pending', 'deleting']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalCacheState",
  "unit": "export"
}
```

</details>
