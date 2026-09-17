# stove0_observer_protocol.ObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationresult:217a483dbf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5e09ef2bde"></a>
- <a id="s-38f5cfe220"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-1de5e934a8"></a>`module`: `stove0_observer_protocol`
- <a id="s-35a4635300"></a>`name`: `ObservationResult`
- <a id="s-df94d07c8a"></a>`unit`: `export`

### Declared structure

- <a id="s-b71a157b26"></a>`kind`: `"class"`
- <a id="s-c08f584bb7"></a>`signature`: `"\"(*, format: Literal['stove0-observation-result/v1'] = 'stove0-observation-result/v1', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['observed', 'inapplicable', 'failed', 'canceled'], observer: stove0_protocol.models.ObserverImplementation, observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], facts_schema: stove0_protocol.models.JsonSchemaDocument \| None = None, facts: dict[str, JsonValue] \| None = None, facts_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, execution_evidence: dict[str, JsonValue] = <factory>, inapplicable: stove0_protocol.models.ObservationInapplicable \| None = None, failure: stove0_protocol.models.ObservationFailure \| None = None, result_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-e6c95c09cc"></a>

- <a id="s-73daf263c5"></a>`type`: `"object"`
- <a id="s-3f631e1d15"></a>`additionalProperties`: `false`
- <a id="s-ea640c700a"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d25f802309"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-0a0963aee8)) |  |
| <a id="s-c7581f3926"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-0a0963aee8))); (type="null")]; default=null |  |
| <a id="s-3c492dbb6c"></a>`facts_schema` | no | anyOf=[([JsonSchemaDocument](#s-b25bca96a1)); (type="null")]; default=null |  |
| <a id="s-82aea2765c"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-2f422d005d"></a>`failure` | no | anyOf=[([ObservationFailure](#s-3fbd69243d)); (type="null")]; default=null |  |
| <a id="s-a69ba8d221"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-b54b0c2987"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-d9082f8294)); (type="null")]; default=null |  |
| <a id="s-73a8807daf"></a>`observer` | yes | [ObserverImplementation](#s-f248fa607b) |  |
| <a id="s-946cfedcf8"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d36d96f6aa"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5cc2b8be6f"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b449377e1"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4a52a9cac3"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-308e5c7b13"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-8277b97cc2)); minItems=1 |  |

##### Definitions

- [ArtifactSubject](#s-8277b97cc2)
- [CollectionId](#s-fb11b6fa4b)
- [CollectionRootRef](#s-e2d115b6ee)
- [JsonSchemaDocument](#s-b25bca96a1)
- [JsonValue](#s-0a0963aee8)
- [ObservationFailure](#s-3fbd69243d)
- [ObservationInapplicable](#s-d9082f8294)
- [ObserverImplementation](#s-f248fa607b)

##### <a id="s-8277b97cc2"></a>definition `ArtifactSubject`

- <a id="s-372e68284e"></a>`type`: `"object"`
- <a id="s-01ab17493a"></a>`additionalProperties`: `false`
- <a id="s-6f0ffd3da5"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b64d1855c0"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-d6857161be"></a>`collection` | yes | [CollectionRootRef](#s-e2d115b6ee) |  |
| <a id="s-0e422b7a22"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-fcf51952af"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-04396c462e"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-99e68a13fb"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-84312822b3"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fb11b6fa4b"></a>definition `CollectionId`

- <a id="s-340b6821ca"></a>`type`: `"integer"`
- <a id="s-7e7dbb0d87"></a>`minimum`: `1`

##### <a id="s-e2d115b6ee"></a>definition `CollectionRootRef`

- <a id="s-b20dac608c"></a>`type`: `"object"`
- <a id="s-0630126f29"></a>`additionalProperties`: `false`
- <a id="s-815713dc04"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e5ef6ec24d"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4c5bfc5f80"></a>`collection_id` | yes | [CollectionId](#s-fb11b6fa4b) |  |
| <a id="s-f166237beb"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b25bca96a1"></a>definition `JsonSchemaDocument`

- <a id="s-fc93a11f6d"></a>`type`: `"object"`
- <a id="s-0c28db24fa"></a>`additionalProperties`: `false`
- <a id="s-b1b71dec52"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d1cbcb304e"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-8fb0003b23"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-edfe836f3e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0d1fdfd159"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-0a0963aee8)) |  |
| <a id="s-596384e970"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0a0963aee8"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-3fbd69243d"></a>definition `ObservationFailure`

- <a id="s-79cff16568"></a>`type`: `"object"`
- <a id="s-89f4a8e604"></a>`additionalProperties`: `false`
- <a id="s-90b0afa146"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e576b56b52"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-00b7f5a4ba"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-cb144458c3"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-d9082f8294"></a>definition `ObservationInapplicable`

- <a id="s-7bfc8e24e0"></a>`type`: `"object"`
- <a id="s-4bb0411d0d"></a>`additionalProperties`: `false`
- <a id="s-ae9142508e"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac221fe7c6"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-016b253f8c"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-f248fa607b"></a>definition `ObserverImplementation`

- <a id="s-685443ffc6"></a>`type`: `"object"`
- <a id="s-703ad73d48"></a>`additionalProperties`: `false`
- <a id="s-147dc326f0"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c76872da77"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-73fd8be961"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-28531c8c51"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-d3c7ea220a"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-2090db7a91"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [canonical_subjects](stove0-observer-protocol-observationresult-canonical-subjects.md)
- [verify_digest](stove0-observer-protocol-observationresult-verify-digest.md)
- [validate_state_payload](stove0-observer-protocol-observationresult-validate-state-payload.md)
- [seal](stove0-observer-protocol-observationresult-seal.md)

## Governing policies

- <a id="pa-9609d61ae1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f46e73d1ba18a84356e3962af81b71b629f0c403fd5bfeb3d37fd7f7e2d36c21 -->

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
        "result_sha256": {
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
        "subjects",
        "result_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-observation-result/v1'] = 'stove0-observation-result/v1', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['observed', 'inapplicable', 'failed', 'canceled'], observer: stove0_protocol.models.ObserverImplementation, observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], facts_schema: stove0_protocol.models.JsonSchemaDocument | None = None, facts: dict[str, JsonValue] | None = None, facts_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, execution_evidence: dict[str, JsonValue] = <factory>, inapplicable: stove0_protocol.models.ObservationInapplicable | None = None, failure: stove0_protocol.models.ObservationFailure | None = None, result_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationResult",
  "unit": "export"
}
```

</details>
