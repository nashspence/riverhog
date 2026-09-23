# stove0_observer_protocol.JsonSchemaValidationProfile.from_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-jsonschemavalida-528ed6802c:926553e208 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1fac3d7f22"></a>
- <a id="s-2994a81eed"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-237c19b0af"></a>`module`: `stove0_observer_protocol`
- <a id="s-19f4f162df"></a>`name`: `from_schema`
- <a id="s-808f6b9603"></a>`owner`: `stove0_observer_protocol.JsonSchemaValidationProfile`
- <a id="s-b12a335385"></a>`unit`: `member`

### Declared structure

- <a id="s-7a4b66d32f"></a>`kind`: `"classmethod"`
- <a id="s-999ea410e0"></a>`signature`: `"\"(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaValidationProfile'\""`

## Maintained corroboration

### Related interface records

- [JsonSchemaValidationProfile](stove0-observer-protocol-jsonschemavalidationprofile.md)

## Governing policies

- <a id="pa-f8687dcb1c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.JsonSchemaValidationProfile.from_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfa3e1cc7784d62a2d6fae63cc6d5aae60b39929af522f87e9c6aa31e98893c7 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaValidationProfile'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "from_schema",
  "owner": "stove0_observer_protocol.JsonSchemaValidationProfile",
  "unit": "member"
}
```

</details>
