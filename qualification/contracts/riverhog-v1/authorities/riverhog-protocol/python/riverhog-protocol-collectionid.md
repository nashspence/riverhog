# riverhog_protocol.CollectionId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionid:3a038c2a42 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff7dcdfca7"></a>
- <a id="s-5bc94a5e9d"></a>`distribution`: `riverhog-protocol`
- <a id="s-4d09a22135"></a>`module`: `riverhog_protocol`
- <a id="s-da657b4a75"></a>`name`: `CollectionId`
- <a id="s-935a0bd608"></a>`unit`: `export`

### Declared structure

- <a id="s-575eb4c71f"></a>`kind`: `"type-alias"`
- <a id="s-05f232b3cf"></a>`value`: `"typing.Annotated[int, BeforeValidator(func=<function CollectionId.<locals>.<lambda>>, json_schema_input_type=PydanticUndefined), FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=1)]), PlainSerializer(func=<function CollectionId.<locals>.<lambda>>, return_type=<class 'str'>, when_used='always'), WithJsonSchema(json_schema={'allOf': [{'type': 'string', 'pattern': '^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\\\s\\\\S])'}, {'not': {'const': '0'}}]}, mode=None)]"`

## Governing policies

- <a id="pa-6167cb7c96"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionId`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5278c09860e2d8f8811942e75ca13e6b4f6de2f71012fb10e0fdf590df5cb627 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[int, BeforeValidator(func=<function CollectionId.<locals>.<lambda>>, json_schema_input_type=PydanticUndefined), FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=1)]), PlainSerializer(func=<function CollectionId.<locals>.<lambda>>, return_type=<class 'str'>, when_used='always'), WithJsonSchema(json_schema={'allOf': [{'type': 'string', 'pattern': '^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\\\s\\\\S])'}, {'not': {'const': '0'}}]}, mode=None)]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionId",
  "unit": "export"
}
```

</details>
