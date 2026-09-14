# riverhog_protocol.CollectionIdParameter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionidparameter:0a18cda640 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-02ff51a854"></a>`value`: `"typing.Annotated[int, FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=1)]), BeforeValidator(func=<function parse_collection_id_parameter>, json_schema_input_type=PydanticUndefined)]"`

## Governing policies

- <a id="pa-89e0d0a859"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionIdParameter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e6688ca7a991b684683cfae1c2c9c4f4e8e492502193caab70f2a79c3d6f249 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[int, FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=1)]), BeforeValidator(func=<function parse_collection_id_parameter>, json_schema_input_type=PydanticUndefined)]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionIdParameter",
  "unit": "export"
}
```
