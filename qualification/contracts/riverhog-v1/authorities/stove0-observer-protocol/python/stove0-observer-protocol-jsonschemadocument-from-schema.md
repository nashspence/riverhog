# stove0_observer_protocol.JsonSchemaDocument.from_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-jsonschemadocume-effa71e72d:a1abb7479d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1792558511"></a>
- <a id="s-66dceff8c9"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-bb2a7248ef"></a>`module`: `stove0_observer_protocol`
- <a id="s-9ecc4a514e"></a>`name`: `from_schema`
- <a id="s-4efecfed0d"></a>`owner`: `stove0_observer_protocol.JsonSchemaDocument`
- <a id="s-6ee2f6514c"></a>`unit`: `member`

### Declared structure

- <a id="s-8872b34ce5"></a>`kind`: `"classmethod"`
- <a id="s-3b1dcc8ab5"></a>`signature`: `"\"(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaDocument'\""`

## Maintained corroboration

### Related interface records

- [JsonSchemaDocument](stove0-observer-protocol-jsonschemadocument.md)

## Governing policies

- <a id="pa-ee9940496b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.JsonSchemaDocument.from_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 336c79b688db7a8b80556e22283a16504ef0cde0f48b173a819d0a747e0edf89 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaDocument'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "from_schema",
  "owner": "stove0_observer_protocol.JsonSchemaDocument",
  "unit": "member"
}
```

</details>
