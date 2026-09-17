# stove0_observer_protocol.ObserverContractPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontractpayload:c3324347ca -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aa2f1bb0b5"></a>
- <a id="s-5229a6af7c"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-87857832c4"></a>`module`: `stove0_observer_protocol`
- <a id="s-642e654a0e"></a>`name`: `ObserverContractPayload`
- <a id="s-b3f88d0044"></a>`unit`: `export`

### Declared structure

- <a id="s-f8c21d1bbb"></a>`kind`: `"class"`
- <a id="s-8f9eb28a0e"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576) -> None\""`

#### Validated model schema

<a id="s-833ddab43b"></a>

- <a id="s-0dbe5b82a0"></a>`type`: `"object"`
- <a id="s-242277a05a"></a>`additionalProperties`: `false`
- <a id="s-ca0448dc8c"></a>`required`: `["id","options_schema","facts_schema","facts_semantics"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5028d5595d"></a>`facts_schema` | yes | [JsonSchemaDocument](#s-7a3bfaee1a) |  |
| <a id="s-3f15958089"></a>`facts_semantics` | yes | [SemanticValidationProfile](#s-c9658308bd) |  |
| <a id="s-caa2cf4a72"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7ed1977bb1"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-978ada5c0c"></a>`options_schema` | yes | [JsonSchemaDocument](#s-7a3bfaee1a) |  |

##### Definitions

- [JsonSchemaDocument](#s-7a3bfaee1a)
- [JsonValue](#s-bd2e069bd0)
- [SemanticValidationProfile](#s-c9658308bd)

##### <a id="s-7a3bfaee1a"></a>definition `JsonSchemaDocument`

- <a id="s-05b9b3b0f7"></a>`type`: `"object"`
- <a id="s-fa245fb474"></a>`additionalProperties`: `false`
- <a id="s-f8aa504962"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6d473a3c1a"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-fbf7278921"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-9bc166d60c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5bef5e9e3f"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-bd2e069bd0)) |  |
| <a id="s-12d8368b80"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bd2e069bd0"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-c9658308bd"></a>definition `SemanticValidationProfile`

- <a id="s-dc460ae53f"></a>`type`: `"object"`
- <a id="s-b689b39dd8"></a>`additionalProperties`: `false`
- <a id="s-6b0f5766c4"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-261a8e504a"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-8404d849ba"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b0b240c706"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3cc3010a79"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [bind_semantic_conformance_vectors](stove0-observer-protocol-observercontractpayload-bind-semantic-conformance-vectors.md)

## Governing policies

- <a id="pa-3992f548ba"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContractPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d4a4d332c1a2f66051fc8259595d61b3ca7cd1ee0815da4224e7a5eae191f05 -->

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
        "facts_semantics"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverContractPayload",
  "unit": "export"
}
```

</details>
