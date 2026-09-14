# riverhog_protocol.CollectionId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionid:3a038c2a42 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-05f232b3cf"></a>`value`: `"typing.Annotated[int, FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=1)]), BeforeValidator(func=<function validate_collection_id>, json_schema_input_type=PydanticUndefined)]"`

## Governing policies

- <a id="pa-6167cb7c96"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionId`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0367a3076f8473afcee44ea9258d99a5b099960158da9aa0a0b6d0ba868c028f -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[int, FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=1)]), BeforeValidator(func=<function validate_collection_id>, json_schema_input_type=PydanticUndefined)]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionId",
  "unit": "export"
}
```
