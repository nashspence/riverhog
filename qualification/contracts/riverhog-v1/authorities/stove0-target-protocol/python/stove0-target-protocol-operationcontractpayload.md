# stove0_target_protocol.OperationContractPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-operationcontractpayload:572ce2bea7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-48a8dc0c02"></a>
- <a id="s-7286bd54a6"></a>`distribution`: `stove0-target-protocol`
- <a id="s-d6d4ac5329"></a>`module`: `stove0_target_protocol`
- <a id="s-0f64d6c859"></a>`name`: `OperationContractPayload`
- <a id="s-8e8d398f9b"></a>`unit`: `export`

### Declared structure

- <a id="s-015b7a960e"></a>`kind`: `"class"`
- <a id="s-aa2c30746b"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaDocument, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaDocument \| None = None, source_retirement_permitted: bool = False) -> None\""`

#### Validated model schema

<a id="s-b957659f61"></a>
- <a id="s-d1bb9ba975"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3387002b7e"></a>`effect_receipt_schema` | no | anyOf=#/$defs/JsonSchemaDocument \| type="null" |  |
| <a id="s-330e994e0c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7fe997f587"></a>`inputs` | yes | type="array"; minItems=1; items=(#/$defs/InputArtifactContract) |  |
| <a id="s-16922a6e7f"></a>`intent_schema` | yes | #/$defs/JsonSchemaDocument |  |
| <a id="s-71741a9710"></a>`intent_semantics` | yes | #/$defs/SemanticValidationProfile |  |
| <a id="s-97e1514b3f"></a>`outputs` | no | type="array"; items=(#/$defs/OutputArtifactContract) |  |
| <a id="s-184719398d"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"] |  |
| <a id="s-109c2556ba"></a>`source_retirement_permitted` | no | type="boolean" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-42aa27240a"></a>`InputArtifactContract` | type="object"; fields=`allowed_dispositions`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-cf4390e523"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-23d22d4639"></a>`JsonValue` | empty object |
| <a id="s-0e33c701a9"></a>`OutputArtifactContract` | type="object"; fields=`derived_from_roles`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-02a4dcadff"></a>`SemanticValidationProfile` | type="object"; fields=`conformance_vectors_sha256`, `id`, `profile_sha256`, `rules`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OperationContractPayload.validate_roles](stove0-target-protocol-operationcontractpayload-validate-roles.md)
- [stove0_target_protocol.OperationContractPayload.bind_semantic_conformance_vectors](stove0-target-protocol-operationcontractpayload-bind-semantic-conformance-vectors.md)

## Governing policies

- <a id="pa-572eff2d20"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OperationContractPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b93d1233e4c91435b34c372e20d88c1fb94b67d67895dead0130d59ca8e4369 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "InputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "allowed_dispositions": {
              "anyOf": [
                {
                  "items": {
                    "enum": [
                      "transformed",
                      "preserved",
                      "omitted",
                      "rejected"
                    ],
                    "type": "string"
                  },
                  "type": "array"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role"
          ],
          "type": "object"
        },
        "JsonSchemaDocument": {
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
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "OutputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "derived_from_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "derived_from_roles"
          ],
          "type": "object"
        },
        "SemanticValidationProfile": {
          "additionalProperties": false,
          "properties": {
            "conformance_vectors_sha256": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "rules": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "id",
            "rules",
            "profile_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "effect_receipt_schema": {
          "anyOf": [
            {
              "$ref": "#/$defs/JsonSchemaDocument"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/InputArtifactContract"
          },
          "minItems": 1,
          "type": "array"
        },
        "intent_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "intent_semantics": {
          "$ref": "#/$defs/SemanticValidationProfile"
        },
        "outputs": {
          "default": [],
          "items": {
            "$ref": "#/$defs/OutputArtifactContract"
          },
          "type": "array"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "type": "string"
        },
        "source_retirement_permitted": {
          "default": false,
          "type": "boolean"
        }
      },
      "required": [
        "id",
        "intent_schema",
        "intent_semantics",
        "inputs"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaDocument, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaDocument | None = None, source_retirement_permitted: bool = False) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OperationContractPayload",
  "unit": "export"
}
```
