# stove0_target_protocol.DepartureEffectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectreceipt:8481faede1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8510a4e93f"></a>
- <a id="s-d24b6b349e"></a>`distribution`: `stove0-target-protocol`
- <a id="s-031a225a4e"></a>`module`: `stove0_target_protocol`
- <a id="s-69ddcab4d6"></a>`name`: `DepartureEffectReceipt`
- <a id="s-b250442f02"></a>`unit`: `export`

### Declared structure

- <a id="s-9bdcbafa84"></a>`kind`: `"class"`
- <a id="s-39c8930547"></a>`signature`: `"\"(*, format: Literal['stove0-departure-effect-receipt/v1'] = 'stove0-departure-effect-receipt/v1', departure_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue], receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-d630c134aa"></a>

- <a id="s-99a5ee1150"></a>`type`: `"object"`
- <a id="s-9d480eb012"></a>`additionalProperties`: `false`
- <a id="s-93db830fa1"></a>`required`: `["departure_id","target_identity","result","receipt_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d609ce0c99"></a>`departure_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f6d1e56943"></a>`format` | no | type="string"; const="stove0-departure-effect-receipt/v1"; default="stove0-departure-effect-receipt/v1" |  |
| <a id="s-53fa4fc07d"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d2b7555b5"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-89af39113b)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-departure-effect-receipt"} |  |
| <a id="s-8c72a68908"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [JsonValue](#s-89af39113b)

##### <a id="s-89af39113b"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [exact_identity](stove0-target-protocol-departureeffectreceipt-exact-identity.md)
- [bounded_result](stove0-target-protocol-departureeffectreceipt-bounded-result.md)
- [seal](stove0-target-protocol-departureeffectreceipt-seal.md)

## Governing policies

- <a id="pa-797af40707"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ffbad9c155a068c3ae2161c5591b1a3ea92af05efb8ef1392abb0dbb270c588 -->

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
        "receipt_sha256": {
          "pattern": "^[0-9a-f]{64}$",
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
        "result",
        "receipt_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-departure-effect-receipt/v1'] = 'stove0-departure-effect-receipt/v1', departure_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue], receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "DepartureEffectReceipt",
  "unit": "export"
}
```

</details>
