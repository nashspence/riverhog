# stove0_target_support.TargetContractPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontractpayload:ccbffdf823 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4de55964a6"></a>
- <a id="s-518911b948"></a>`distribution`: `stove0-target-support`
- <a id="s-51e0b64c52"></a>`module`: `stove0_target_support`
- <a id="s-b2c37db9f0"></a>`name`: `TargetContractPayload`
- <a id="s-d55b8d75cc"></a>`unit`: `export`

### Declared structure

- <a id="s-d4668649b6"></a>`kind`: `"class"`
- <a id="s-b090f56282"></a>`signature`: `"\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-f812059820"></a>

- <a id="s-f265c493a9"></a>`type`: `"object"`
- <a id="s-709be52ece"></a>`additionalProperties`: `false`
- <a id="s-38c82ff4ea"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","operations"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-84aa4e55b9"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2e57132227"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fa79fb7691"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-20ef86a128"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-fa9f791a75)); minItems=1 |  |
| <a id="s-018317609b"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-a9cdf32be8"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-22fdbc16a6"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-0746bb6069)
- [JsonValue](#s-6586730d4d)
- [TargetOperationSupport](#s-fa9f791a75)

##### <a id="s-0746bb6069"></a>definition `JsonSchemaValidationProfile`

- <a id="s-785358a875"></a>`type`: `"object"`
- <a id="s-5d3aee652c"></a>`additionalProperties`: `false`
- <a id="s-464fc31342"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35743bda18"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-c7475a3d9d"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-fa9640b353"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-366dd4cc23"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-91de9fb781"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-6586730d4d)) |  |

##### <a id="s-6586730d4d"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-fa9f791a75"></a>definition `TargetOperationSupport`

- <a id="s-c3bada6646"></a>`type`: `"object"`
- <a id="s-10b7a3f785"></a>`additionalProperties`: `false`
- <a id="s-43059c6147"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5806c50c3a"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d7bf265462"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bbc49ba83f"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-0746bb6069) |  |
| <a id="s-06bb66e9cc"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

## Maintained corroboration

### Related interface records

- [bind_result_kind](stove0-target-support-targetcontractpayload-bind-result-kind.md)
- [canonical_operations](stove0-target-support-targetcontractpayload-canonical-operations.md)

## Governing policies

- <a id="pa-4f06eb5170"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContractPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0e65dd616148eb1ad612fa499eddadf637453509ac4f989b71f1f6468a9b747 -->

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
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetContractPayload",
  "unit": "export"
}
```

</details>
