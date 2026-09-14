# stove0_observer_protocol.ObserverContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontract:7f8d7fbee0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-085164d5b9"></a>
- <a id="s-7b80c6fc11"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-afdbacd651"></a>`module`: `stove0_observer_protocol`
- <a id="s-a319b4b75c"></a>`name`: `ObserverContract`
- <a id="s-c0dccc94bf"></a>`unit`: `export`

### Declared structure

- <a id="s-bbe7f6e787"></a>`kind`: `"class"`
- <a id="s-3c4ff68ef7"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-2009529b23"></a>
- <a id="s-b9cdd6874f"></a>`title`: ObserverContract
- <a id="s-2d3dce9162"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ffb388d74b"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-79265b7bba"></a>`facts_schema` | yes | #/$defs/JsonSchemaDocument |  |
| <a id="s-0c0d2b0f9d"></a>`facts_semantics` | yes | #/$defs/SemanticValidationProfile |  |
| <a id="s-57f9615963"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-22b0547d5b"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864 |  |
| <a id="s-135a1e34f9"></a>`options_schema` | yes | #/$defs/JsonSchemaDocument |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-80f7422792"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-7449c72b12"></a>`JsonValue` | empty object |
| <a id="s-f92abed4ae"></a>`SemanticValidationProfile` | type="object"; fields=`conformance_vectors_sha256`, `id`, `profile_sha256`, `rules`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObserverContract.verify_digest](stove0-observer-protocol-observercontract-verify-digest.md)
- [stove0_observer_protocol.ObserverContract.seal](stove0-observer-protocol-observercontract-seal.md)

## Governing policies

- <a id="pa-a7d8c4c37e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39b14f39e26f65ef8e573333d95aa7cb43798fc68baca3d3e46a0904dc7746f7 -->

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
        "JsonValue": {},
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
              "default": null,
              "title": "Conformance Vectors Sha256"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Profile Sha256",
              "type": "string"
            },
            "rules": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "title": "Rules",
              "type": "array"
            }
          },
          "required": [
            "id",
            "rules",
            "profile_sha256"
          ],
          "title": "SemanticValidationProfile",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
          "type": "string"
        },
        "facts_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "facts_semantics": {
          "$ref": "#/$defs/SemanticValidationProfile"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        }
      },
      "required": [
        "id",
        "options_schema",
        "facts_schema",
        "facts_semantics",
        "contract_sha256"
      ],
      "title": "ObserverContract",
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverContract",
  "unit": "export"
}
```
