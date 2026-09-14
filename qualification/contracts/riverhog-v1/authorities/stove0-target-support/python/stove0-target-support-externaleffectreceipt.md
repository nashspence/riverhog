# stove0_target_support.ExternalEffectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-externaleffectreceipt:70110147a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7398b0c175"></a>
- <a id="s-fc89aae5d5"></a>`distribution`: `stove0-target-support`
- <a id="s-638fef1efb"></a>`module`: `stove0_target_support`
- <a id="s-a883c0abd6"></a>`name`: `ExternalEffectReceipt`
- <a id="s-c819c73f09"></a>`unit`: `export`

### Declared structure

- <a id="s-a6888fa4cf"></a>`kind`: `"class"`
- <a id="s-b931044e97"></a>`signature`: `"\"(*, format: Literal['stove0-external-effect-receipt/v1'] = 'stove0-external-effect-receipt/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue], receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-2c5acd8e17"></a>
- <a id="s-25d4318ede"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5dd28c68b0"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7f552a4218"></a>`format` | no | type="string"; const="stove0-external-effect-receipt/v1" |  |
| <a id="s-57a6bf2957"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-82888c67bc"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2d8b0bb99e"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c30cedc7d6"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cf8353041b"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-43c6d1cd1f"></a>`result` | yes | type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-d84a9310f6"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-5c74bb8515"></a>`JsonValue` | empty object |

## Maintained corroboration

### Related interface records

- [stove0_target_support.ExternalEffectReceipt.verify_digest](stove0-target-support-externaleffectreceipt-verify-digest.md)
- [stove0_target_support.ExternalEffectReceipt.seal](stove0-target-support-externaleffectreceipt-seal.md)

## Governing policies

- <a id="pa-5808dbee77"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.ExternalEffectReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b52a1f1b431397890401930eb96a5ac75f827b7555b1108a0ebf36eb65f20025 -->

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
        "receipt_sha256": {
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
        "result",
        "receipt_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-external-effect-receipt/v1'] = 'stove0-external-effect-receipt/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue], receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "ExternalEffectReceipt",
  "unit": "export"
}
```
