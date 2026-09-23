# stove0_target_support.TargetDescriptorPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdescriptorpayload:abb8680543 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5914b15ed3"></a>
- <a id="s-28e7546a14"></a>`distribution`: `stove0-target-support`
- <a id="s-18b4d4fc31"></a>`module`: `stove0_target_support`
- <a id="s-8a2a2dd0ef"></a>`name`: `TargetDescriptorPayload`
- <a id="s-5fd5d89a62"></a>`unit`: `export`

### Declared structure

- <a id="s-7a18687b6c"></a>`kind`: `"class"`
- <a id="s-5d0b713b6e"></a>`signature`: `"\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-d327d0a7f1"></a>

- <a id="s-40908bfe95"></a>`type`: `"object"`
- <a id="s-2240330163"></a>`additionalProperties`: `false`
- <a id="s-eeadf0be6b"></a>`required`: `["implementation_id","implementation_version","source_revision","image_id","operations"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2008929d6"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$" |  |
| <a id="s-36325e7294"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f20149652a"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-61920b3b8b"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-35cc60d226)); minItems=1 |  |
| <a id="s-a075524e40"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-300a6238ee"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-447ad02e34"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-9717d62431)
- [JsonValue](#s-6db153df21)
- [TargetOperationSupport](#s-35cc60d226)

##### <a id="s-9717d62431"></a>definition `JsonSchemaValidationProfile`

- <a id="s-b265a745a5"></a>`type`: `"object"`
- <a id="s-5bf1a67f3f"></a>`additionalProperties`: `false`
- <a id="s-934fa38ced"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-65048a94c6"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-d46e9b446d"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-dbede7a7cc"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-86f7c57fd4"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-60eb5b3ff4"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-6db153df21)) |  |

##### <a id="s-6db153df21"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-35cc60d226"></a>definition `TargetOperationSupport`

- <a id="s-ac53575cc8"></a>`type`: `"object"`
- <a id="s-c59c53be78"></a>`additionalProperties`: `false`
- <a id="s-d3f039e1e5"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6019f437b6"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-764e0cbb37"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-726bdaa859"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-9717d62431) |  |
| <a id="s-be85f3d5f4"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

## Maintained corroboration

### Related interface records

- [canonical_operations](stove0-target-support-targetdescriptorpayload-canonical-operations.md)
- [bind_result_kind](stove0-target-support-targetdescriptorpayload-bind-result-kind.md)

## Governing policies

- <a id="pa-14ed9ca059"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDescriptorPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ed3829067d6e37224c4cdd4b3ff01850ea7d565800a7bad08036b814f706180 -->

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
        "image_id": {
          "pattern": "^sha256:[0-9a-f]{64}$",
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
        "image_id",
        "operations"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetDescriptorPayload",
  "unit": "export"
}
```

</details>
