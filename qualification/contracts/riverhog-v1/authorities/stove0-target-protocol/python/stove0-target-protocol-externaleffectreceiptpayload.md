# stove0_target_protocol.ExternalEffectReceiptPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-externaleffectreceiptpayload:2badaeee3c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-17fa7c8b9f"></a>
- <a id="s-405451ae6a"></a>`distribution`: `stove0-target-protocol`
- <a id="s-7ed4a98ec0"></a>`module`: `stove0_target_protocol`
- <a id="s-638d9e47f0"></a>`name`: `ExternalEffectReceiptPayload`
- <a id="s-fd6af42653"></a>`unit`: `export`

### Declared structure

- <a id="s-c4eeb453a2"></a>`kind`: `"class"`
- <a id="s-03bdb053ce"></a>`signature`: `"\"(*, format: Literal['stove0-external-effect-receipt/v1'] = 'stove0-external-effect-receipt/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-ef6c1dadef"></a>

- <a id="s-b2306808b3"></a>`type`: `"object"`
- <a id="s-93be9f2bc4"></a>`additionalProperties`: `false`
- <a id="s-faf907d87c"></a>`required`: `["job_id","request_sha256","target_contract_sha256","operation_contract_sha256","plan_sha256","execution_sha256","result"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-268f278e53"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-91a45315f1"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1"; default="stove0-external-effect-receipt/v1" |  |
| <a id="s-232e746948"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9af6677f28"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-806174901e"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6ef3a64691"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ec51b29310"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-5066a7c85e)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-external-effect-receipt"} |  |
| <a id="s-45f72f8330"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [JsonValue](#s-5066a7c85e)

##### <a id="s-5066a7c85e"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [bounded_result](stove0-target-protocol-externaleffectreceiptpayload-bounded-result.md)

## Governing policies

- <a id="pa-59854b7232"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.ExternalEffectReceiptPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc039ed31f6d31bc1a4cef10c268f28fe15ce50e96eea4fee6228aaef4ccba68 -->

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
        "execution_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "format": {
          "const": "stove0-external-effect-receipt/v1",
          "default": "stove0-external-effect-receipt/v1",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "request_sha256": {
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
            "reason": "bounded-external-effect-receipt"
          }
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "request_sha256",
        "target_contract_sha256",
        "operation_contract_sha256",
        "plan_sha256",
        "execution_sha256",
        "result"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-external-effect-receipt/v1'] = 'stove0-external-effect-receipt/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "ExternalEffectReceiptPayload",
  "unit": "export"
}
```

</details>
