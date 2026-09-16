# stove0_observer_protocol.ObserverDescriptorPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerdescriptorpayload:d89286f625 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e50183a98e"></a>
- <a id="s-ebe5e502f5"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-6cd091d081"></a>`module`: `stove0_observer_protocol`
- <a id="s-20cb2ac692"></a>`name`: `ObserverDescriptorPayload`
- <a id="s-b272ca1f7c"></a>`unit`: `export`

### Declared structure

- <a id="s-76180ccf59"></a>`kind`: `"class"`
- <a id="s-cd3a55f765"></a>`signature`: `"\"(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], contracts: Annotated[tuple[stove0_protocol.models.ObserverContractSupport, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-ccdb5bd0b5"></a>

- <a id="s-253a1cc132"></a>`type`: `"object"`
- <a id="s-c67705b326"></a>`additionalProperties`: `false`
- <a id="s-f47c5e9a8d"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","contracts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95302954e6"></a>`contracts` | yes | type="array"; items=([ObserverContractSupport](#s-df3a202597)); minItems=1 |  |
| <a id="s-d543b03b28"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9ea38094e5"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-97f73c491e"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-a414c1ddb9"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-64cf842729"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |

##### Definitions

- [JsonSchemaDocument](#s-1a236a0ddd)
- [JsonValue](#s-469b54f0f5)
- [ObserverContractSupport](#s-df3a202597)
- [SemanticValidationProfile](#s-94db257f5b)

##### <a id="s-1a236a0ddd"></a>definition `JsonSchemaDocument`

- <a id="s-a56bfbce4b"></a>`type`: `"object"`
- <a id="s-d1af2bf3d4"></a>`additionalProperties`: `false`
- <a id="s-c1e80178db"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-46989e7829"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-cdbc134652"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-0daf4b9f98"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a2657ce0bf"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-469b54f0f5)) |  |
| <a id="s-c3615f1480"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-469b54f0f5"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-df3a202597"></a>definition `ObserverContractSupport`

- <a id="s-3bc0b24885"></a>`type`: `"object"`
- <a id="s-42c88b4bab"></a>`additionalProperties`: `false`
- <a id="s-e85d3575f3"></a>`required`: `["contract_id","contract_sha256","options_schema","facts_schema","facts_semantics","maximum_result_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a941d2d8fc"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b87a887aa0"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-91ff24e641"></a>`facts_schema` | yes | [JsonSchemaDocument](#s-1a236a0ddd) |  |
| <a id="s-7cc8645ce6"></a>`facts_semantics` | yes | [SemanticValidationProfile](#s-94db257f5b) |  |
| <a id="s-7aebb17b65"></a>`maximum_result_bytes` | yes | type="integer"; minimum=1; maximum=67108864 |  |
| <a id="s-c02ece24b6"></a>`options_schema` | yes | [JsonSchemaDocument](#s-1a236a0ddd) |  |
| <a id="s-e00d97c368"></a>`preferred_subject_batch_size` | no | type="integer"; minimum=1; default=128 |  |

##### <a id="s-94db257f5b"></a>definition `SemanticValidationProfile`

- <a id="s-e89b6f3c88"></a>`type`: `"object"`
- <a id="s-8361cae0c6"></a>`additionalProperties`: `false`
- <a id="s-29b77fed58"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-34800abe87"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-f1fc10b3d3"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d1d664e0df"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d23ffb8dd9"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [unique_contracts](stove0-observer-protocol-observerdescriptorpayload-unique-contracts.md)

## Governing policies

- <a id="pa-83085a237c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverDescriptorPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f41bb60218a38daff60cff64e601be2eaf5a6b70d83ec13a65e18dcc8353bc1 -->

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
        "ObserverContractSupport": {
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
              "$ref": "#/$defs/JsonSchemaDocument"
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
              "$ref": "#/$defs/JsonSchemaDocument"
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
        "contracts": {
          "items": {
            "$ref": "#/$defs/ObserverContractSupport"
          },
          "minItems": 1,
          "type": "array"
        },
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
        "protocol": {
          "const": "stove0-content-observer/v1",
          "default": "stove0-content-observer/v1",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_digest",
        "contracts"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], contracts: Annotated[tuple[stove0_protocol.models.ObserverContractSupport, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverDescriptorPayload",
  "unit": "export"
}
```

</details>
