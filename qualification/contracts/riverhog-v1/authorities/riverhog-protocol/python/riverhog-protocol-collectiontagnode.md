# riverhog_protocol.CollectionTagNode

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagnode:37149abd5b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-828ade08b3"></a>
- <a id="s-b66beb27fc"></a>`distribution`: `riverhog-protocol`
- <a id="s-73ccb36f87"></a>`module`: `riverhog_protocol`
- <a id="s-b757839c0d"></a>`name`: `CollectionTagNode`
- <a id="s-d61d7621cf"></a>`unit`: `export`

### Declared structure

- <a id="s-cdc13116be"></a>`kind`: `"class"`
- <a id="s-d3b8dda32f"></a>`signature`: `"\"(prefix: 'bytes', tag: 'bytes \| None' = None, children: 'tuple[CollectionTagChild, ...]' = ()) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-08dd399313"></a>`prefix` | `'bytes'` | `required` |
| <a id="s-12e03f182d"></a>`tag` | `'bytes \| None'` | `None` |
| <a id="s-7851943cef"></a>`children` | `'tuple[CollectionTagChild, ...]'` | `()` |

## Governing policies

- <a id="pa-1dbf4a39a0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagNode`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
