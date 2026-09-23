# stove0_target_support.TargetOperationSupport

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetoperationsupport:3423f36d07 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63d66a64c4"></a>
- <a id="s-b972c1ad89"></a>`distribution`: `stove0-target-support`
- <a id="s-a63486b2e6"></a>`module`: `stove0_target_support`
- <a id="s-79496fcf91"></a>`name`: `TargetOperationSupport`
- <a id="s-ccd4bb4476"></a>`unit`: `export`

### Declared structure

- <a id="s-5b6a100cff"></a>`kind`: `"class"`
- <a id="s-9dd3c930e3"></a>`signature`: `"\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', options_schema: stove0_protocol.models.JsonSchemaValidationProfile) -> None\""`

#### Validated model schema

<a id="s-dc993b1b17"></a>

- <a id="s-2b8fae9415"></a>`type`: `"object"`
- <a id="s-dc89e27af7"></a>`additionalProperties`: `false`
- <a id="s-31241fc9db"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cce4d5b1d8"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-64f99e5d7c"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b737583ea3"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-bc3eb8eece) |  |
| <a id="s-81b564fb09"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-bc3eb8eece)
- [JsonValue](#s-4eda7d957f)

##### <a id="s-bc3eb8eece"></a>definition `JsonSchemaValidationProfile`

- <a id="s-594fd0c4d2"></a>`type`: `"object"`
- <a id="s-503758e354"></a>`additionalProperties`: `false`
- <a id="s-38c9636d3c"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ad0767861"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-457511e02f"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-2b3995f48c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fc9b5096c9"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8d8abe28e5"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-4eda7d957f)) |  |

##### <a id="s-4eda7d957f"></a>definition `JsonValue`

- Accepts: any JSON value.

## Governing policies

- <a id="pa-6ad76327f4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetOperationSupport`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5de6dd776b92fb367debf1e70d3f5fc2d028e8cca0fe68a767975390d774f2fb -->

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
        "JsonValue": {}
      },
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
    },
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', options_schema: stove0_protocol.models.JsonSchemaValidationProfile) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetOperationSupport",
  "unit": "export"
}
```

</details>
