# riverhog_protocol.CollectionIdParameter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionidparameter:0a18cda640 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ea4d5d3291"></a>
- <a id="s-c009a7fd5c"></a>`distribution`: `riverhog-protocol`
- <a id="s-a8c546b042"></a>`module`: `riverhog_protocol`
- <a id="s-e036b7cc67"></a>`name`: `CollectionIdParameter`
- <a id="s-1fa408b313"></a>`unit`: `export`

### Declared structure

- <a id="s-f24280dfd4"></a>`kind`: `"type-alias"`
- <a id="s-02ff51a854"></a>`value`: `"typing.Annotated[int, FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=1), Le(le=9223372036854775807)]), BeforeValidator(func=<function parse_collection_id_parameter>, json_schema_input_type=PydanticUndefined), WithJsonSchema(json_schema={'allOf': [{'type': 'string', 'pattern': '^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\\\s\\\\S])'}, {'not': {'const': '0'}}]}, mode=None)]"`

## Governing policies

- <a id="pa-89e0d0a859"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionIdParameter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb25bd2ac0b92528f5f89f267ba9f8eb40e718b50aa939e3d7918402fed70555 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[int, FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=1), Le(le=9223372036854775807)]), BeforeValidator(func=<function parse_collection_id_parameter>, json_schema_input_type=PydanticUndefined), WithJsonSchema(json_schema={'allOf': [{'type': 'string', 'pattern': '^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\\\s\\\\S])'}, {'not': {'const': '0'}}]}, mode=None)]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionIdParameter",
  "unit": "export"
}
```

</details>
