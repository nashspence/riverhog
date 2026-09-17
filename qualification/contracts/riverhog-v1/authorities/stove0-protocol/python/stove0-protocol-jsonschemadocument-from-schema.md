# stove0_protocol.JsonSchemaDocument.from_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-jsonschemadocument-from-schema:7fa772ba42 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-605c5e07ab"></a>
- <a id="s-e06c19156e"></a>`distribution`: `stove0-protocol`
- <a id="s-bb678b1639"></a>`module`: `stove0_protocol`
- <a id="s-681e286ad1"></a>`name`: `from_schema`
- <a id="s-a9d635f432"></a>`owner`: `stove0_protocol.JsonSchemaDocument`
- <a id="s-1e9dc50efe"></a>`unit`: `member`

### Declared structure

- <a id="s-e28ff24cfd"></a>`kind`: `"classmethod"`
- <a id="s-5a11a1a247"></a>`signature`: `"\"(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaDocument'\""`

## Maintained corroboration

### Related interface records

- [JsonSchemaDocument](stove0-protocol-jsonschemadocument.md)

## Governing policies

- <a id="pa-f9084459c6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JsonSchemaDocument.from_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cad0793668cf132152d02fb36442a8185e7e105c95c5b11963ad362512486c68 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaDocument'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_schema",
  "owner": "stove0_protocol.JsonSchemaDocument",
  "unit": "member"
}
```

</details>
