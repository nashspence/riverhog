# riverhog_protocol.CollectionTagChild

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagchild:a479e369d5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9df3c8aa6c"></a>
- <a id="s-1d1496de87"></a>`distribution`: `riverhog-protocol`
- <a id="s-234a17bc18"></a>`module`: `riverhog_protocol`
- <a id="s-bbaffe8399"></a>`name`: `CollectionTagChild`
- <a id="s-d747464133"></a>`unit`: `export`

### Declared structure

- <a id="s-98d1e1c23e"></a>`kind`: `"class"`
- <a id="s-a90f3a4478"></a>`signature`: `"\"(label: 'int', digest: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-471d0811d9"></a>`label` | `'int'` | `required` |
| <a id="s-39201efc69"></a>`digest` | `'str'` | `required` |

## Governing policies

- <a id="pa-60b8083034"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagChild`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dbe2256d986951686ceb38415f79cad14e14742e163e02744997a118aa9142fd -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "label",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "digest",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(label: 'int', digest: 'str') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionTagChild",
  "unit": "export"
}
```

</details>
