# stove0_target_support.TargetPreflightRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetpreflightrequest:04c2391293 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3fe4ab393c"></a>
- <a id="s-3c1d31d5bc"></a>`distribution`: `stove0-target-support`
- <a id="s-11310ffaa4"></a>`module`: `stove0_target_support`
- <a id="s-a456e004d6"></a>`name`: `TargetPreflightRequest`
- <a id="s-802df7be1a"></a>`unit`: `export`

### Declared structure

- <a id="s-cb8b3bf67c"></a>`kind`: `"class"`
- <a id="s-04e232baa8"></a>`signature`: `"\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-5c87ff72e1"></a>

- <a id="s-943f4b19de"></a>`type`: `"object"`
- <a id="s-f256e0d976"></a>`additionalProperties`: `false`
- <a id="s-03f0b767a4"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7c26093bbf"></a>`inputs` | yes | [TargetInputAuthority](#s-60a5d6ef7e) |  |
| <a id="s-54f20c0f57"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-d95af0d2da)) |  |
| <a id="s-605db37c31"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-54fec26db4)) |  |
| <a id="s-c388713b52"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e72f9aca93"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8119716746"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-55e1c184fb"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-d95af0d2da)) |  |

##### Definitions

- [ArtifactSelectionRef](#s-39ad60087b)
- [ArtifactSubject](#s-f072295446)
- [CollectionId](#s-e027a9025e)
- [CollectionRootRef](#s-8191dbc46c)
- [JsonSchemaValidationProfile](#s-10bf8e2657)
- [JsonValue](#s-d95af0d2da)
- [ObservationEvidence](#s-54fec26db4)
- [ObservationFailure](#s-7b3261407b)
- [ObservationInapplicable](#s-dd71a88a10)
- [ObservationRequest](#s-46662bba44)
- [ObservationResult](#s-07f8b4ea0f)
- [ObserverImplementation](#s-78ed3d8bbb)
- [TargetInputAuthority](#s-60a5d6ef7e)
- [TargetInputRoleCount](#s-769600fdb9)

##### <a id="s-39ad60087b"></a>definition `ArtifactSelectionRef`

- <a id="s-c681e325e2"></a>`type`: `"object"`
- <a id="s-b7dd6b86ee"></a>`additionalProperties`: `false`
- <a id="s-aa5fb3b8f4"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-716544fe32"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-3aab4adfd3"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-44ef421401"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-f072295446"></a>definition `ArtifactSubject`

- <a id="s-1df0087a74"></a>`type`: `"object"`
- <a id="s-0e34d1507c"></a>`additionalProperties`: `false`
- <a id="s-da454ac718"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9e81e626cf"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-6c3c610190"></a>`collection` | yes | [CollectionRootRef](#s-8191dbc46c) |  |
| <a id="s-5078849c51"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-5f806c319f"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-3403f3a526"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-1e1b421301"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2bd6acfb9c"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e027a9025e"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-a63e579a63"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-5c9814f0a5"></a>2 | not=(const="0") |

##### <a id="s-8191dbc46c"></a>definition `CollectionRootRef`

- <a id="s-dec5b5a88c"></a>`type`: `"object"`
- <a id="s-5f3d230056"></a>`additionalProperties`: `false`
- <a id="s-8e4d37763c"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed0e6548df"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-99e160ddce"></a>`collection_id` | yes | [CollectionId](#s-e027a9025e) |  |
| <a id="s-9157d45b6b"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-10bf8e2657"></a>definition `JsonSchemaValidationProfile`

- <a id="s-a308534de0"></a>`type`: `"object"`
- <a id="s-d64cc76425"></a>`additionalProperties`: `false`
- <a id="s-a15df944a9"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2946f620ca"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-fbebf8b0a7"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-50f19dcf65"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e835ce48e8"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ed53fc8482"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-d95af0d2da)) |  |

##### <a id="s-d95af0d2da"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-54fec26db4"></a>definition `ObservationEvidence`

- <a id="s-31a5da7602"></a>`type`: `"object"`
- <a id="s-590e430c5c"></a>`additionalProperties`: `false`
- <a id="s-fb740bfdbf"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6a829b53a2"></a>`request` | yes | [ObservationRequest](#s-46662bba44) |  |
| <a id="s-c2f11b6152"></a>`result` | yes | [ObservationResult](#s-07f8b4ea0f) |  |

##### <a id="s-7b3261407b"></a>definition `ObservationFailure`

- <a id="s-1d8a5c1938"></a>`type`: `"object"`
- <a id="s-6d649bdd67"></a>`additionalProperties`: `false`
- <a id="s-672396781b"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-59a41d56fd"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f28f8ff79a"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-91b954dc85"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-dd71a88a10"></a>definition `ObservationInapplicable`

- <a id="s-c304ad841b"></a>`type`: `"object"`
- <a id="s-6b8c22c458"></a>`additionalProperties`: `false`
- <a id="s-b6f920355b"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e56ff48357"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ffc476b31a"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-46662bba44"></a>definition `ObservationRequest`

- <a id="s-c6c212e838"></a>`type`: `"object"`
- <a id="s-223bff7d99"></a>`additionalProperties`: `false`
- <a id="s-41d4d5c5ea"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7912155451"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-cea4ea0f6e"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-79c82b5b15"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e808e5a2fb"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-087faa60d1"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-846fa4c119"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-6579935f16"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-d95af0d2da)) |  |
| <a id="s-81d5dcd2fc"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b3bf1d53fe"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-f3ff7209aa"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-f072295446)); minItems=1 |  |
| <a id="s-ccafda1b7f"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-87f04bf3cb"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-07f8b4ea0f"></a>definition `ObservationResult`

- <a id="s-b6afb6a70b"></a>`type`: `"object"`
- <a id="s-b9b995a4e2"></a>`additionalProperties`: `false`
- <a id="s-6e9e5ed063"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-531ccc2722"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-d95af0d2da)) |  |
| <a id="s-fe49470f10"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-d95af0d2da))); (type="null")]; default=null |  |
| <a id="s-74e0f662b2"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-10bf8e2657)); (type="null")]; default=null |  |
| <a id="s-98d311e69c"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-6461790541"></a>`failure` | no | anyOf=[([ObservationFailure](#s-7b3261407b)); (type="null")]; default=null |  |
| <a id="s-b841b1dc99"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-16955be89e"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-dd71a88a10)); (type="null")]; default=null |  |
| <a id="s-39c8eff626"></a>`observer` | yes | [ObserverImplementation](#s-78ed3d8bbb) |  |
| <a id="s-e77ea25c7a"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ca722d037c"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8d582dbd24"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-21993bf60d"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-510d18e25c"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-1d9a2434d0"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-f072295446)); minItems=1 |  |

##### <a id="s-78ed3d8bbb"></a>definition `ObserverImplementation`

- <a id="s-01fa96f66a"></a>`type`: `"object"`
- <a id="s-066da2f48c"></a>`additionalProperties`: `false`
- <a id="s-cc1134579b"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eeb6eb4aca"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b669abbf29"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ea43677312"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-e462f5d3ae"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-6421cfbffe"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-60a5d6ef7e"></a>definition `TargetInputAuthority`

- <a id="s-92e014c41c"></a>`type`: `"object"`
- <a id="s-5341ca8a52"></a>`additionalProperties`: `false`
- <a id="s-c1869947f8"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c3f519ad3c"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-769600fdb9)); minItems=1 |  |
| <a id="s-1afc0ffe52"></a>`selection` | yes | [ArtifactSelectionRef](#s-39ad60087b) |  |

##### <a id="s-769600fdb9"></a>definition `TargetInputRoleCount`

- <a id="s-9bf08cd9d0"></a>`type`: `"object"`
- <a id="s-21cb8dbedd"></a>`additionalProperties`: `false`
- <a id="s-0fc6ce6629"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-faa1fd20ec"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-753f62b9be"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [canonical_observations](stove0-target-support-targetpreflightrequest-canonical-observations.md)

## Governing policies

- <a id="pa-ca92e2d251"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetPreflightRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52ea3e07c5ea9d91efb95fb357253104637810ed97c8c7f079abe6b023c6f1d1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelectionRef": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "selection_sha256",
            "artifact_count",
            "total_bytes"
          ],
          "type": "object"
        },
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
        "ObservationEvidence": {
          "additionalProperties": false,
          "properties": {
            "request": {
              "$ref": "#/$defs/ObservationRequest"
            },
            "result": {
              "$ref": "#/$defs/ObservationResult"
            }
          },
          "required": [
            "request",
            "result"
          ],
          "type": "object"
        },
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
        "ObservationRequest": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-observation-request/v1",
              "default": "stove0-observation-request/v1",
              "type": "string"
            },
            "maximum_result_bytes": {
              "default": 1048576,
              "maximum": 67108864,
              "minimum": 1,
              "type": "integer"
            },
            "observer_contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "observer_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "observer_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "observer_registration_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
              "type": "string"
            },
            "options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "request_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "subjects": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
              "type": "array"
            },
            "timeout_seconds": {
              "default": 300,
              "maximum": 86400,
              "minimum": 1,
              "type": "integer"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "work_id",
            "observer_registration_id",
            "observer_descriptor_sha256",
            "observer_contract_id",
            "observer_contract_sha256",
            "subjects",
            "request_id"
          ],
          "type": "object"
        },
        "ObservationResult": {
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
        "TargetInputAuthority": {
          "additionalProperties": false,
          "properties": {
            "roles": {
              "items": {
                "$ref": "#/$defs/TargetInputRoleCount"
              },
              "minItems": 1,
              "type": "array"
            },
            "selection": {
              "$ref": "#/$defs/ArtifactSelectionRef"
            }
          },
          "required": [
            "selection",
            "roles"
          ],
          "type": "object"
        },
        "TargetInputRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "count"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "inputs": {
          "$ref": "#/$defs/TargetInputAuthority"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "observations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ObservationEvidence"
          },
          "type": "array"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "type": "string"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "inputs",
        "intent"
      ],
      "type": "object"
    },
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = ()) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetPreflightRequest",
  "unit": "export"
}
```

</details>
