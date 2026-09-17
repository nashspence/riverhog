# stove0_observer_protocol.ObserverContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontract:7f8d7fbee0 -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-2d3dce9162"></a>`type`: `"object"`
- <a id="s-56911f8456"></a>`additionalProperties`: `false`
- <a id="s-85e23c11ec"></a>`required`: `["id","options_schema","facts_schema","facts_semantics","contract_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ffb388d74b"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-79265b7bba"></a>`facts_schema` | yes | [JsonSchemaDocument](#s-80f7422792) |  |
| <a id="s-0c0d2b0f9d"></a>`facts_semantics` | yes | [SemanticValidationProfile](#s-f92abed4ae) |  |
| <a id="s-57f9615963"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-22b0547d5b"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-135a1e34f9"></a>`options_schema` | yes | [JsonSchemaDocument](#s-80f7422792) |  |

##### Definitions

- [JsonSchemaDocument](#s-80f7422792)
- [JsonValue](#s-7449c72b12)
- [SemanticValidationProfile](#s-f92abed4ae)

##### <a id="s-80f7422792"></a>definition `JsonSchemaDocument`

- <a id="s-885d3faceb"></a>`type`: `"object"`
- <a id="s-71813fa7ed"></a>`additionalProperties`: `false`
- <a id="s-def4f41fb6"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c0135546d9"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-384a56a38b"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-cbb831b552"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-58607a0e0b"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-7449c72b12)) |  |
| <a id="s-3ecb285c1d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-7449c72b12"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-f92abed4ae"></a>definition `SemanticValidationProfile`

- <a id="s-fd5705c138"></a>`type`: `"object"`
- <a id="s-f653ac5d86"></a>`additionalProperties`: `false`
- <a id="s-64118c5a3d"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c702e1b40f"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-d53d4be94a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-614b7c9012"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cdece9f918"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [verify_digest](stove0-observer-protocol-observercontract-verify-digest.md)
- [bind_semantic_conformance_vectors](stove0-observer-protocol-observercontract-bind-semantic-conformance-vectors.md)
- [seal](stove0-observer-protocol-observercontract-seal.md)

## Governing policies

- <a id="pa-a7d8c4c37e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b9fbd5b10efdd7c9f748a4ca08c6c4eab36047374f9235865caeae62794c700 -->

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
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
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
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
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

</details>
