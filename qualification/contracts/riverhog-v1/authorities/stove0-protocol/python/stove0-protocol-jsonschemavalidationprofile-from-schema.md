# stove0_protocol.JsonSchemaValidationProfile.from_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-jsonschemavalidationprofi-ecb6602cc6:05ed623dc7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d92009bf9a"></a>
- <a id="s-7367472ee1"></a>`distribution`: `stove0-protocol`
- <a id="s-7f1f4c4fe1"></a>`module`: `stove0_protocol`
- <a id="s-135f46ba62"></a>`name`: `from_schema`
- <a id="s-30a53d3a75"></a>`owner`: `stove0_protocol.JsonSchemaValidationProfile`
- <a id="s-bd8b4354e1"></a>`unit`: `member`

### Declared structure

- <a id="s-30f53651e8"></a>`kind`: `"classmethod"`
- <a id="s-0cc2121592"></a>`signature`: `"\"(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaValidationProfile'\""`

## Maintained corroboration

### Related interface records

- [JsonSchemaValidationProfile](stove0-protocol-jsonschemavalidationprofile.md)

## Governing policies

- <a id="pa-348dd1b5aa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JsonSchemaValidationProfile.from_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7bba8bd6058f67379ed9e24b01a3b8f056368859342c10fbb7382344952c8ce -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaValidationProfile'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_schema",
  "owner": "stove0_protocol.JsonSchemaValidationProfile",
  "unit": "member"
}
```

</details>
