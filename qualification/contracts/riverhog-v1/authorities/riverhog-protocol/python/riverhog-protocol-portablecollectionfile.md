# riverhog_protocol.PortableCollectionFile

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectionfile:0e282bb990 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-903c1f9a1a"></a>
- <a id="s-4eb4e006a3"></a>`distribution`: `riverhog-protocol`
- <a id="s-b23ec362e5"></a>`module`: `riverhog_protocol`
- <a id="s-c16fec3746"></a>`name`: `PortableCollectionFile`
- <a id="s-aa959ba59c"></a>`unit`: `export`

### Declared structure

- <a id="s-3e5df00484"></a>`kind`: `"class"`
- <a id="s-c95114c58d"></a>`signature`: `"\"(path: 'str', bytes: 'int', sha256: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-df02fc00a6"></a>`path` | `'str'` | `required` |
| <a id="s-01244c8fb3"></a>`bytes` | `'int'` | `required` |
| <a id="s-83e3a5924d"></a>`sha256` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [from_mapping](riverhog-protocol-portablecollectionfile-from-mapping.md)
- [to_mapping](riverhog-protocol-portablecollectionfile-to-mapping.md)

## Governing policies

- <a id="pa-644e507db4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionFile`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e359622499d7c136a77fbc3c56a4ade3515d99008075dec8051934e1cac6d73 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(path: 'str', bytes: 'int', sha256: 'str') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionFile",
  "unit": "export"
}
```

</details>
