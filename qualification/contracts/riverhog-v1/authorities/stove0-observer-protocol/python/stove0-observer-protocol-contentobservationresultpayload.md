# stove0_observer_protocol.ContentObservationResultPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-39d9233ae0:7fea932d49 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3f267e9c30"></a>
- <a id="s-3375463a6c"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-9acd5ada77"></a>`module`: `stove0_observer_protocol`
- <a id="s-819b36cd61"></a>`name`: `ContentObservationResultPayload`
- <a id="s-ee9695e630"></a>`unit`: `export`

### Declared structure

- <a id="s-f452fe3f32"></a>`kind`: `"class"`
- <a id="s-1389c9e375"></a>`signature`: `"\"(*, format: Literal['stove0-observation-result/v1'] = 'stove0-observation-result/v1', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['observed', 'inapplicable', 'failed', 'canceled'], observer: stove0_protocol.models.ObserverImplementation, observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.WorkArtifactSubject, ...], MinLen(min_length=1)], facts_schema: stove0_protocol.models.JsonSchemaValidationProfile \| None = None, facts: dict[str, JsonValue] \| None = None, facts_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, execution_evidence: dict[str, JsonValue] = <factory>, inapplicable: stove0_protocol.models.ContentObservationInapplicable \| None = None, failure: stove0_protocol.models.ContentObservationFailure \| None = None) -> None\""`

#### Validated model schema

<a id="s-5974614185"></a>

- <a id="s-692e46264c"></a>`type`: `"object"`
- <a id="s-4303c1cba3"></a>`additionalProperties`: `false`
- <a id="s-53140c8b85"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cad5d7b8bc"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-e5c5eb7687)) |  |
| <a id="s-4c13dcd9e1"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-e5c5eb7687))); (type="null")]; default=null |  |
| <a id="s-4b47e61b05"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-5caa072672)); (type="null")]; default=null |  |
| <a id="s-1cfab35706"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-f4406461b1"></a>`failure` | no | anyOf=[([ContentObservationFailure](#s-434be5afc2)); (type="null")]; default=null |  |
| <a id="s-d739a41a5b"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-5e9b17fa29"></a>`inapplicable` | no | anyOf=[([ContentObservationInapplicable](#s-a5b7142241)); (type="null")]; default=null |  |
| <a id="s-48682be8c8"></a>`observer` | yes | [ObserverImplementation](#s-906f8c9ba0) |  |
| <a id="s-b2326d1b9d"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4936fb57ce"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ed4e375507"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-296ea318f6"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-72759608bd"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-4181f89f12)); minItems=1 |  |

##### Definitions

- [CollectionId](#s-d626f3d5dc)
- [CollectionRootIdentityRef](#s-c452939674)
- [ContentObservationFailure](#s-434be5afc2)
- [ContentObservationInapplicable](#s-a5b7142241)
- [JsonSchemaValidationProfile](#s-5caa072672)
- [JsonValue](#s-e5c5eb7687)
- [ObserverImplementation](#s-906f8c9ba0)
- [WorkArtifactSubject](#s-4181f89f12)

##### <a id="s-d626f3d5dc"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-d7b90c0afa"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-62b5c88d8a"></a>2 | not=(const="0") |

##### <a id="s-c452939674"></a>definition `CollectionRootIdentityRef`

- <a id="s-eb150769f9"></a>`type`: `"object"`
- <a id="s-d393e58eb4"></a>`additionalProperties`: `false`
- <a id="s-6d843d0139"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-30b7e24074"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2d41f9c645"></a>`collection_id` | yes | [CollectionId](#s-d626f3d5dc) |  |
| <a id="s-a534b74952"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-434be5afc2"></a>definition `ContentObservationFailure`

- <a id="s-b6d8d17980"></a>`type`: `"object"`
- <a id="s-be9c6067c6"></a>`additionalProperties`: `false`
- <a id="s-b6e8b255be"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-368f9f4a51"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4df499047e"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-de5e716d36"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-a5b7142241"></a>definition `ContentObservationInapplicable`

- <a id="s-dc310fec17"></a>`type`: `"object"`
- <a id="s-5bc59c1db6"></a>`additionalProperties`: `false`
- <a id="s-2e68dec54f"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9fde4a154f"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e351ca0a33"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-5caa072672"></a>definition `JsonSchemaValidationProfile`

- <a id="s-b5a003b800"></a>`type`: `"object"`
- <a id="s-d07f030423"></a>`additionalProperties`: `false`
- <a id="s-6ce7df29d2"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-251a498d1d"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-7b420595db"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-a52950ff8e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d00f15100b"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-008aecaa59"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-e5c5eb7687)) |  |

##### <a id="s-e5c5eb7687"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-906f8c9ba0"></a>definition `ObserverImplementation`

- <a id="s-cca8b4f7a4"></a>`type`: `"object"`
- <a id="s-f179696c45"></a>`additionalProperties`: `false`
- <a id="s-7e4b4dfbed"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2dbe084623"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-14b4638096"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c6d7790967"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-3289802e0b"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-bcbb1672d9"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-4181f89f12"></a>definition `WorkArtifactSubject`

- <a id="s-75c1ae163e"></a>`type`: `"object"`
- <a id="s-8a1fef4a90"></a>`additionalProperties`: `false`
- <a id="s-4222767ad9"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d0b7dc715"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-98dd1e6292"></a>`collection` | yes | [CollectionRootIdentityRef](#s-c452939674) |  |
| <a id="s-6d6655b2da"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-b8d0ce230f"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-1986334678"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-060a1b595c"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7adecbc39f"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [validate_state_payload](stove0-observer-protocol-contentobservationresultpayload-validate-state-payload.md)
- [canonical_subjects](stove0-observer-protocol-contentobservationresultpayload-canonical-subjects.md)

## Governing policies

- <a id="pa-948ab4658f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationResultPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 11d0493e617a3e5dd47866d5875e1c3fd743dae3a1647d51947b464fe440484b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
        },
        "CollectionRootIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        },
        "ContentObservationFailure": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "retryable": {
              "type": "boolean"
            }
          },
          "required": [
            "code",
            "message",
            "retryable"
          ],
          "type": "object"
        },
        "ContentObservationInapplicable": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "type": "object"
        },
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
        "ObserverImplementation": {
          "additionalProperties": false,
          "properties": {
            "descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
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
            },
            "version": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "id",
            "version",
            "source_revision",
            "descriptor_sha256"
          ],
          "type": "object"
        },
        "WorkArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootIdentityRef"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
              "type": "string"
            },
            "media_type": {
              "anyOf": [
                {
                  "maxLength": 255,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "role",
            "collection",
            "path",
            "bytes",
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "execution_evidence": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "facts": {
          "anyOf": [
            {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "facts_schema": {
          "anyOf": [
            {
              "$ref": "#/$defs/JsonSchemaValidationProfile"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "facts_sha256": {
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
        "failure": {
          "anyOf": [
            {
              "$ref": "#/$defs/ContentObservationFailure"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "format": {
          "const": "stove0-observation-result/v1",
          "default": "stove0-observation-result/v1",
          "type": "string"
        },
        "inapplicable": {
          "anyOf": [
            {
              "$ref": "#/$defs/ContentObservationInapplicable"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "observer": {
          "$ref": "#/$defs/ObserverImplementation"
        },
        "observer_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "observer_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "state": {
          "enum": [
            "observed",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "type": "string"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/WorkArtifactSubject"
          },
          "minItems": 1,
          "type": "array"
        }
      },
      "required": [
        "request_id",
        "state",
        "observer",
        "observer_contract_id",
        "observer_contract_sha256",
        "subjects"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-observation-result/v1'] = 'stove0-observation-result/v1', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['observed', 'inapplicable', 'failed', 'canceled'], observer: stove0_protocol.models.ObserverImplementation, observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.WorkArtifactSubject, ...], MinLen(min_length=1)], facts_schema: stove0_protocol.models.JsonSchemaValidationProfile | None = None, facts: dict[str, JsonValue] | None = None, facts_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, execution_evidence: dict[str, JsonValue] = <factory>, inapplicable: stove0_protocol.models.ContentObservationInapplicable | None = None, failure: stove0_protocol.models.ContentObservationFailure | None = None) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ContentObservationResultPayload",
  "unit": "export"
}
```

</details>
