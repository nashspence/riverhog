# stove0_observer_protocol.ObserverContractSupport

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observercontractsupport:6ab5904f17 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0c4092e9ce"></a>
- <a id="s-ce4449aac9"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-c6a128bcdc"></a>`module`: `stove0_observer_protocol`
- <a id="s-211057bac1"></a>`name`: `ObserverContractSupport`
- <a id="s-a5c8b802a9"></a>`unit`: `export`

### Declared structure

- <a id="s-4afc55be91"></a>`kind`: `"class"`
- <a id="s-dfc5ddf530"></a>`signature`: `"\"(*, contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaValidationProfile, facts_schema: stove0_protocol.models.JsonSchemaValidationProfile, facts_semantics: stove0_protocol.models.SemanticValidationProfile, preferred_subject_batch_size: Annotated[int, Ge(ge=1)] = 128, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)]) -> None\""`

#### Validated model schema

<a id="s-ae0445f5da"></a>

- <a id="s-ae5a1b9147"></a>`type`: `"object"`
- <a id="s-a345443802"></a>`additionalProperties`: `false`
- <a id="s-9211a35372"></a>`required`: `["contract_id","contract_sha256","options_schema","facts_schema","facts_semantics","maximum_result_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-62076d4003"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a9330be9c3"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-369416659a"></a>`facts_schema` | yes | [JsonSchemaValidationProfile](#s-a5ae99ec61) |  |
| <a id="s-4d7cf97761"></a>`facts_semantics` | yes | [SemanticValidationProfile](#s-7f1ee1f478) |  |
| <a id="s-86c186b52d"></a>`maximum_result_bytes` | yes | type="integer"; minimum=1; maximum=67108864 |  |
| <a id="s-eac157b46b"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-a5ae99ec61) |  |
| <a id="s-6569433014"></a>`preferred_subject_batch_size` | no | type="integer"; minimum=1; default=128 |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-a5ae99ec61)
- [JsonValue](#s-be1d98ce0e)
- [SemanticValidationProfile](#s-7f1ee1f478)

##### <a id="s-a5ae99ec61"></a>definition `JsonSchemaValidationProfile`

- <a id="s-22ab8c248a"></a>`type`: `"object"`
- <a id="s-d15bc9c558"></a>`additionalProperties`: `false`
- <a id="s-4cacfb9111"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-49b733a0b7"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-5b690b5553"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-1395d872cd"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-401ba4d454"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-793c5f428c"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-be1d98ce0e)) |  |

##### <a id="s-be1d98ce0e"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-7f1ee1f478"></a>definition `SemanticValidationProfile`

- <a id="s-02e6d507d6"></a>`type`: `"object"`
- <a id="s-1af8d2a795"></a>`additionalProperties`: `false`
- <a id="s-509b7eb021"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-77095f663d"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-2f7a271318"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-005cbf53bf"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-526abc6b1d"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [from_contract](stove0-observer-protocol-observercontractsupport-from-contract.md)

## Governing policies

- <a id="pa-44784ea4a6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverContractSupport`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ceca5ef699862e12abadaf9ccb6ff2c0b5c3c68a9dd739e189d3b82a55fcaf3a -->

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
        "contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "facts_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "facts_semantics": {
          "$ref": "#/$defs/SemanticValidationProfile"
        },
        "maximum_result_bytes": {
          "maximum": 67108864,
          "minimum": 1,
          "type": "integer"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaValidationProfile"
        },
        "preferred_subject_batch_size": {
          "default": 128,
          "minimum": 1,
          "type": "integer"
        }
      },
      "required": [
        "contract_id",
        "contract_sha256",
        "options_schema",
        "facts_schema",
        "facts_semantics",
        "maximum_result_bytes"
      ],
      "type": "object"
    },
    "signature": "\"(*, contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaValidationProfile, facts_schema: stove0_protocol.models.JsonSchemaValidationProfile, facts_semantics: stove0_protocol.models.SemanticValidationProfile, preferred_subject_batch_size: Annotated[int, Ge(ge=1)] = 128, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverContractSupport",
  "unit": "export"
}
```

</details>
