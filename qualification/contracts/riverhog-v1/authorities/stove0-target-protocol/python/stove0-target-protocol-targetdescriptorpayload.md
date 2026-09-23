# stove0_target_protocol.TargetDescriptorPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdescriptorpayload:0188b82fbc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d851e8ace"></a>
- <a id="s-9b071ae45e"></a>`distribution`: `stove0-target-protocol`
- <a id="s-1f173b2a27"></a>`module`: `stove0_target_protocol`
- <a id="s-0b31db1ef0"></a>`name`: `TargetDescriptorPayload`
- <a id="s-8cbd78884e"></a>`unit`: `export`

### Declared structure

- <a id="s-63ac65d19e"></a>`kind`: `"class"`
- <a id="s-eff7bc2496"></a>`signature`: `"\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-b11ee639c1"></a>

- <a id="s-aae8405f07"></a>`type`: `"object"`
- <a id="s-95ceefda18"></a>`additionalProperties`: `false`
- <a id="s-7409d4a1d9"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","operations"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c9af9446c"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f0d963044d"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1ae28656b0"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-5b497a100e"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-c23e711272)); minItems=1 |  |
| <a id="s-61358300f3"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-fa5c7a084a"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-1f0f676761"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-561425ecbb)
- [JsonValue](#s-0ed8191bf2)
- [TargetOperationSupport](#s-c23e711272)

##### <a id="s-561425ecbb"></a>definition `JsonSchemaValidationProfile`

- <a id="s-ca8fb8f712"></a>`type`: `"object"`
- <a id="s-2a1de8ad59"></a>`additionalProperties`: `false`
- <a id="s-3a9bff2096"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38f7e1370d"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-c89d2852f7"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-395c1ca951"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-82f9c20c21"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f93cd231f2"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-0ed8191bf2)) |  |

##### <a id="s-0ed8191bf2"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-c23e711272"></a>definition `TargetOperationSupport`

- <a id="s-f357fed12d"></a>`type`: `"object"`
- <a id="s-9090fe937e"></a>`additionalProperties`: `false`
- <a id="s-7ac0979419"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-301a6f43f9"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d2d1343a48"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-80c885bc64"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-561425ecbb) |  |
| <a id="s-593c993530"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

## Maintained corroboration

### Related interface records

- [bind_result_kind](stove0-target-protocol-targetdescriptorpayload-bind-result-kind.md)
- [canonical_operations](stove0-target-protocol-targetdescriptorpayload-canonical-operations.md)

## Governing policies

- <a id="pa-19be2309b0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDescriptorPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aebcbbad538c8321d78b1088b5cf812d99cf7a7ac82642815aee3bf67b1e0fa1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonSchemaValidationProfile": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "profile_sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "TargetOperationSupport": {
          "additionalProperties": false,
          "properties": {
            "operation_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "options_schema": {
              "$ref": "#/$defs/JsonSchemaValidationProfile"
            },
            "result_kind": {
              "default": "collection",
              "enum": [
                "collection",
                "external-effect"
              ],
              "type": "string"
            }
          },
          "required": [
            "operation_id",
            "operation_contract_sha256",
            "options_schema"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "type": "string"
        },
        "operations": {
          "items": {
            "$ref": "#/$defs/TargetOperationSupport"
          },
          "minItems": 1,
          "type": "array"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_digest",
        "operations"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetDescriptorPayload",
  "unit": "export"
}
```

</details>
