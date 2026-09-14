# stove0_target_support.TargetOperationSupport

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetoperationsupport:3423f36d07 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-9dd3c930e3"></a>`signature`: `"\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', options_schema: stove0_protocol.models.JsonSchemaDocument) -> None\""`

#### Validated model schema

<a id="s-dc993b1b17"></a>
- <a id="s-1b52c96e6b"></a>`title`: TargetOperationSupport
- <a id="s-2b8fae9415"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cce4d5b1d8"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-64f99e5d7c"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b737583ea3"></a>`options_schema` | yes | #/$defs/JsonSchemaDocument |  |
| <a id="s-81b564fb09"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-822d6c4940"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-4eda7d957f"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-6ad76327f4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetOperationSupport`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b4eecb3c4be0248995b01008355893e2358aabc28938a6fd8f220f79a40e30c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonSchemaDocument": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "title": "Dialect",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "title": "Format Policy",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Schema",
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "title": "JsonSchemaDocument",
          "type": "object"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "title": "Result Kind",
          "type": "string"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "options_schema"
      ],
      "title": "TargetOperationSupport",
      "type": "object"
    },
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', options_schema: stove0_protocol.models.JsonSchemaDocument) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetOperationSupport",
  "unit": "export"
}
```
