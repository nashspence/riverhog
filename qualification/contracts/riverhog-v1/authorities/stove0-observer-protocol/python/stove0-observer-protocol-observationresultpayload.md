# stove0_observer_protocol.ObservationResultPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationresultpayload:0624787402 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-51d99ad36b"></a>
- <a id="s-8110911b5b"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-8204a0dd3f"></a>`module`: `stove0_observer_protocol`
- <a id="s-f1470c0c51"></a>`name`: `ObservationResultPayload`
- <a id="s-0316a2bcdf"></a>`unit`: `export`

### Declared structure

- <a id="s-123cb2083e"></a>`kind`: `"class"`
- <a id="s-7235f0d2aa"></a>`signature`: `"\"(*, format: Literal['stove0-observation-result/v1'] = 'stove0-observation-result/v1', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['observed', 'inapplicable', 'failed', 'canceled'], observer: stove0_protocol.models.ObserverImplementation, observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], facts_schema: stove0_protocol.models.JsonSchemaDocument \| None = None, facts: dict[str, JsonValue] \| None = None, facts_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, execution_evidence: dict[str, JsonValue] = <factory>, inapplicable: stove0_protocol.models.ObservationInapplicable \| None = None, failure: stove0_protocol.models.ObservationFailure \| None = None) -> None\""`

#### Validated model schema

<a id="s-021ce1ada6"></a>

- <a id="s-deedd3f39f"></a>`type`: `"object"`
- <a id="s-d1f8da531d"></a>`additionalProperties`: `false`
- <a id="s-49d320f2f3"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e055628c3"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-f9aa3a9c3c)) |  |
| <a id="s-5aa0ee3812"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-f9aa3a9c3c))); (type="null")]; default=null |  |
| <a id="s-b539e9f399"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-df19d8c006)); (type="null")]; default=null |  |
| <a id="s-1b672888eb"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-5e22481dcd"></a>`failure` | no | anyOf=[([ObservationFailure](#s-1bd4e79cf5)); (type="null")]; default=null |  |
| <a id="s-707b4adcd5"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-fd6bca3715"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-f8f7672fdd)); (type="null")]; default=null |  |
| <a id="s-f64abe282d"></a>`observer` | yes | [ObserverImplementation](#s-4e82d6a5bd) |  |
| <a id="s-6bcb9af691"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6efb5f5caa"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-95c5a696f4"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ae1397f173"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-31e85fc077"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-c7c51d960e)); minItems=1 |  |

##### Definitions

- [ArtifactSubject](#s-c7c51d960e)
- [CollectionId](#s-e17cf34172)
- [CollectionRootRef](#s-c67ac7bc46)
- [JsonSchemaDocument](#s-df19d8c006)
- [JsonValue](#s-f9aa3a9c3c)
- [ObservationFailure](#s-1bd4e79cf5)
- [ObservationInapplicable](#s-f8f7672fdd)
- [ObserverImplementation](#s-4e82d6a5bd)

##### <a id="s-c7c51d960e"></a>definition `ArtifactSubject`

- <a id="s-7b64ef5776"></a>`type`: `"object"`
- <a id="s-897a2649e4"></a>`additionalProperties`: `false`
- <a id="s-b9512086e9"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-244a76f0b1"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-2a4f286294"></a>`collection` | yes | [CollectionRootRef](#s-c67ac7bc46) |  |
| <a id="s-8509c8632b"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-09b4fb06ce"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-603bc219b6"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-324a557d5b"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7b23c8a6d6"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e17cf34172"></a>definition `CollectionId`

- <a id="s-6a46d09b74"></a>`type`: `"integer"`
- <a id="s-1bff6a8988"></a>`minimum`: `1`

##### <a id="s-c67ac7bc46"></a>definition `CollectionRootRef`

- <a id="s-5232a4038b"></a>`type`: `"object"`
- <a id="s-c027c19c85"></a>`additionalProperties`: `false`
- <a id="s-a3bb2d3d46"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d00d2b726"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f81163f616"></a>`collection_id` | yes | [CollectionId](#s-e17cf34172) |  |
| <a id="s-be91c07d91"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-df19d8c006"></a>definition `JsonSchemaDocument`

- <a id="s-541e3d1082"></a>`type`: `"object"`
- <a id="s-11b917f5fb"></a>`additionalProperties`: `false`
- <a id="s-1e4688c1a5"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-367f44981f"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-9b126d5dc9"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-2be7e9e7e5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9a79b90656"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-f9aa3a9c3c)) |  |
| <a id="s-e0cfb352bc"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f9aa3a9c3c"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-1bd4e79cf5"></a>definition `ObservationFailure`

- <a id="s-5f4129cfe0"></a>`type`: `"object"`
- <a id="s-7489eeba6d"></a>`additionalProperties`: `false`
- <a id="s-d7991f6bfd"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4364a5c8a5"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-769b625f8a"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-9ba4b9809c"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-f8f7672fdd"></a>definition `ObservationInapplicable`

- <a id="s-e59e167f90"></a>`type`: `"object"`
- <a id="s-db441c0d49"></a>`additionalProperties`: `false`
- <a id="s-1bd724e771"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c5717b5ad"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1739e8ed08"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-4e82d6a5bd"></a>definition `ObserverImplementation`

- <a id="s-fdc8a703b1"></a>`type`: `"object"`
- <a id="s-3a80ce9645"></a>`additionalProperties`: `false`
- <a id="s-8101cf13da"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f3f9b0aef2"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0f8e2cf277"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5c2119c4b4"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-3234dbd1af"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-9ed856eed2"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [validate_state_payload](stove0-observer-protocol-observationresultpayload-validate-state-payload.md)
- [canonical_subjects](stove0-observer-protocol-observationresultpayload-canonical-subjects.md)

## Governing policies

- <a id="pa-e90156a667"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationResultPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17a5aed5bc1da7b3e9db2cdbf704d0b0ff6b0a567a79783101180a928c205775 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootRef"
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
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "CollectionRootRef": {
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
        "ObservationFailure": {
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
        "ObservationInapplicable": {
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
              "$ref": "#/$defs/JsonSchemaDocument"
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
              "$ref": "#/$defs/ObservationFailure"
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
              "$ref": "#/$defs/ObservationInapplicable"
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
            "$ref": "#/$defs/ArtifactSubject"
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
    "signature": "\"(*, format: Literal['stove0-observation-result/v1'] = 'stove0-observation-result/v1', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['observed', 'inapplicable', 'failed', 'canceled'], observer: stove0_protocol.models.ObserverImplementation, observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], facts_schema: stove0_protocol.models.JsonSchemaDocument | None = None, facts: dict[str, JsonValue] | None = None, facts_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, execution_evidence: dict[str, JsonValue] = <factory>, inapplicable: stove0_protocol.models.ObservationInapplicable | None = None, failure: stove0_protocol.models.ObservationFailure | None = None) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationResultPayload",
  "unit": "export"
}
```

</details>
