# stove0_target_protocol.DepartureEffectReceiptPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectreceiptpayload:2ff1f5c083 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75253d9ac2"></a>
- <a id="s-8d170da269"></a>`distribution`: `stove0-target-protocol`
- <a id="s-fd196bc505"></a>`module`: `stove0_target_protocol`
- <a id="s-e54449dbf4"></a>`name`: `DepartureEffectReceiptPayload`
- <a id="s-307a6fa6a8"></a>`unit`: `export`

### Declared structure

- <a id="s-9debc0f215"></a>`kind`: `"class"`
- <a id="s-d9e663c07e"></a>`signature`: `"\"(*, format: Literal['stove0-departure-effect-receipt/v1'] = 'stove0-departure-effect-receipt/v1', departure_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-42ccbf7fd7"></a>

- <a id="s-2a5d0de5a5"></a>`type`: `"object"`
- <a id="s-e83adf87e8"></a>`additionalProperties`: `false`
- <a id="s-efd46e92b4"></a>`required`: `["departure_id","target_identity","result"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0f19a59766"></a>`departure_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0e25cfae9c"></a>`format` | no | type="string"; const="stove0-departure-effect-receipt/v1"; default="stove0-departure-effect-receipt/v1" |  |
| <a id="s-85035dcdb3"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-204bba7a66)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-departure-effect-receipt"} |  |
| <a id="s-866badead9"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [JsonValue](#s-204bba7a66)

##### <a id="s-204bba7a66"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [bounded_result](stove0-target-protocol-departureeffectreceiptpayload-bounded-result.md)

## Governing policies

- <a id="pa-cddb890408"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectReceiptPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3dc7ce6cb263f2b34cc7412dfd02c26b832932c498ac2286bcf938636b5115cd -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "departure_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "format": {
          "const": "stove0-departure-effect-receipt/v1",
          "default": "stove0-departure-effect-receipt/v1",
          "type": "string"
        },
        "result": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object",
          "x-riverhog-encoded-bytes-max": 65536,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-departure-effect-receipt"
          }
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "departure_id",
        "target_identity",
        "result"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-departure-effect-receipt/v1'] = 'stove0-departure-effect-receipt/v1', departure_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "DepartureEffectReceiptPayload",
  "unit": "export"
}
```

</details>
