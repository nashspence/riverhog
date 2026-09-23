# stove0_target_protocol.TargetPreflightRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetpreflightrequest:ed9797936f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f3b6918081"></a>
- <a id="s-3f980450f2"></a>`distribution`: `stove0-target-protocol`
- <a id="s-1835afa490"></a>`module`: `stove0_target_protocol`
- <a id="s-7274214529"></a>`name`: `TargetPreflightRequest`
- <a id="s-40f58e04fd"></a>`unit`: `export`

### Declared structure

- <a id="s-6a911c9089"></a>`kind`: `"class"`
- <a id="s-96e6a1e1b5"></a>`signature`: `"\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = ()) -> None\""`

#### Validated model schema

<a id="s-3e1c3d234e"></a>

- <a id="s-ff92c76dba"></a>`type`: `"object"`
- <a id="s-5c95cc67b9"></a>`additionalProperties`: `false`
- <a id="s-8836237436"></a>`required`: `["operation_id","operation_contract_sha256","inputs","intent"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-55681a8912"></a>`inputs` | yes | [TargetInputAuthority](#s-4bcd34a677) |  |
| <a id="s-f5822bf4ee"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-b07bd79b40)) |  |
| <a id="s-0a4aff42ad"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](#s-c071276504)) |  |
| <a id="s-c20eea393c"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-afd1762ae2"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-5189f32a95"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-15c2d13618"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b07bd79b40)) |  |

##### Definitions

- [ArtifactSelectionRef](#s-70244300ef)
- [ArtifactSubject](#s-52cd136326)
- [CollectionId](#s-e10c464c86)
- [CollectionRootRef](#s-52f3dd709e)
- [JsonSchemaValidationProfile](#s-fcca8091d8)
- [JsonValue](#s-b07bd79b40)
- [ObservationEvidence](#s-c071276504)
- [ObservationFailure](#s-6643934188)
- [ObservationInapplicable](#s-619805b038)
- [ObservationRequest](#s-af21cf6bdf)
- [ObservationResult](#s-0cdbd450c7)
- [ObserverImplementation](#s-de422889e2)
- [TargetInputAuthority](#s-4bcd34a677)
- [TargetInputRoleCount](#s-6d18b2604a)

##### <a id="s-70244300ef"></a>definition `ArtifactSelectionRef`

- <a id="s-b45e6dfe13"></a>`type`: `"object"`
- <a id="s-66e026336e"></a>`additionalProperties`: `false`
- <a id="s-8224218634"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a7fb4b1b3e"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-ec39cebda4"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3d1e613152"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-52cd136326"></a>definition `ArtifactSubject`

- <a id="s-42c39b4c49"></a>`type`: `"object"`
- <a id="s-9e53ec9d17"></a>`additionalProperties`: `false`
- <a id="s-bd768d36b0"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-36791e1580"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-32c3d5badb"></a>`collection` | yes | [CollectionRootRef](#s-52f3dd709e) |  |
| <a id="s-66e6a2a87f"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-b7267a8ebd"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-f1ace41c95"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-b06ddd7648"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ac04f27c93"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e10c464c86"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-09d10b0933"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-0a6448ddc0"></a>2 | not=(const="0") |

##### <a id="s-52f3dd709e"></a>definition `CollectionRootRef`

- <a id="s-518155fc09"></a>`type`: `"object"`
- <a id="s-dada1c5518"></a>`additionalProperties`: `false`
- <a id="s-8d6567071f"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c1def53639"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-69430a90d5"></a>`collection_id` | yes | [CollectionId](#s-e10c464c86) |  |
| <a id="s-85f02a5887"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-fcca8091d8"></a>definition `JsonSchemaValidationProfile`

- <a id="s-b9c2fc9087"></a>`type`: `"object"`
- <a id="s-6df0943028"></a>`additionalProperties`: `false`
- <a id="s-05289a2eef"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e553f24d1"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-04de122f2b"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-5d15796550"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3bc7818544"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-05411bccf0"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-b07bd79b40)) |  |

##### <a id="s-b07bd79b40"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-c071276504"></a>definition `ObservationEvidence`

- <a id="s-59e732b8de"></a>`type`: `"object"`
- <a id="s-7826dc4153"></a>`additionalProperties`: `false`
- <a id="s-2eb21c8fa6"></a>`required`: `["request","result"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1baf5a0a4c"></a>`request` | yes | [ObservationRequest](#s-af21cf6bdf) |  |
| <a id="s-23b32b8020"></a>`result` | yes | [ObservationResult](#s-0cdbd450c7) |  |

##### <a id="s-6643934188"></a>definition `ObservationFailure`

- <a id="s-315684442f"></a>`type`: `"object"`
- <a id="s-b51c3a56c0"></a>`additionalProperties`: `false`
- <a id="s-3ae334a5d8"></a>`required`: `["code","message","retryable"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bc9eb8853"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0eb8937fb5"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-ee8fae0d5d"></a>`retryable` | yes | type="boolean" |  |

##### <a id="s-619805b038"></a>definition `ObservationInapplicable`

- <a id="s-87a574b46b"></a>`type`: `"object"`
- <a id="s-bf0bd93946"></a>`additionalProperties`: `false`
- <a id="s-ec85650127"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-584291324d"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-070a977185"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

##### <a id="s-af21cf6bdf"></a>definition `ObservationRequest`

- <a id="s-aeef5a0b73"></a>`type`: `"object"`
- <a id="s-975c23db36"></a>`additionalProperties`: `false`
- <a id="s-6973622cce"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e16902a9b"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-d852f1c235"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-33353e9e83"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-353f3ca818"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d33e2bb414"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bbf0bc6c65"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-c1dff95706"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-b07bd79b40)) |  |
| <a id="s-6c0dda253b"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2200329986"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-e0329ecf37"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-52cd136326)); minItems=1 |  |
| <a id="s-721e24287d"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-dcfc026bfa"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-0cdbd450c7"></a>definition `ObservationResult`

- <a id="s-b4739f514c"></a>`type`: `"object"`
- <a id="s-a921336afe"></a>`additionalProperties`: `false`
- <a id="s-7022cd26d3"></a>`required`: `["request_id","state","observer","observer_contract_id","observer_contract_sha256","subjects","result_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ceb61a4da0"></a>`execution_evidence` | no | type="object"; additionalProperties=([JsonValue](#s-b07bd79b40)) |  |
| <a id="s-18cc894547"></a>`facts` | no | anyOf=[(type="object"; additionalProperties=([JsonValue](#s-b07bd79b40))); (type="null")]; default=null |  |
| <a id="s-81410456fa"></a>`facts_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-fcca8091d8)); (type="null")]; default=null |  |
| <a id="s-956a0e8ff9"></a>`facts_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-55b8bb976a"></a>`failure` | no | anyOf=[([ObservationFailure](#s-6643934188)); (type="null")]; default=null |  |
| <a id="s-6eed34a1c8"></a>`format` | no | type="string"; const="stove0-observation-result/v1"; default="stove0-observation-result/v1" |  |
| <a id="s-6861ff9458"></a>`inapplicable` | no | anyOf=[([ObservationInapplicable](#s-619805b038)); (type="null")]; default=null |  |
| <a id="s-3b678805e4"></a>`observer` | yes | [ObserverImplementation](#s-de422889e2) |  |
| <a id="s-d15a68a403"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a7c773a451"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4fa112220a"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-144f4ac706"></a>`result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-488ef6e63c"></a>`state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| <a id="s-0ab0f145a1"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-52cd136326)); minItems=1 |  |

##### <a id="s-de422889e2"></a>definition `ObserverImplementation`

- <a id="s-bb25fd25ce"></a>`type`: `"object"`
- <a id="s-bd9df13009"></a>`additionalProperties`: `false`
- <a id="s-dad5789227"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a9f6a71e5"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ecc5f83a61"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8d0c482190"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-f99c7ec0a2"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-3c4f12812a"></a>`version` | yes | type="string"; maxLength=120; minLength=1 |  |

##### <a id="s-4bcd34a677"></a>definition `TargetInputAuthority`

- <a id="s-de9e95ff4f"></a>`type`: `"object"`
- <a id="s-a2c7531249"></a>`additionalProperties`: `false`
- <a id="s-af1d40f21f"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72cef663b4"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-6d18b2604a)); minItems=1 |  |
| <a id="s-9bb8d5302e"></a>`selection` | yes | [ArtifactSelectionRef](#s-70244300ef) |  |

##### <a id="s-6d18b2604a"></a>definition `TargetInputRoleCount`

- <a id="s-98196f9c29"></a>`type`: `"object"`
- <a id="s-ea1d2ce1db"></a>`additionalProperties`: `false`
- <a id="s-d53e2befae"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f25ddf72f8"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-ac36d3aa53"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [canonical_observations](stove0-target-protocol-targetpreflightrequest-canonical-observations.md)

## Governing policies

- <a id="pa-235ed44fd9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetPreflightRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aebccc3e9a4934eb506b2e4787c005baaecf7948c60762f0861ee968d05a2dd1 -->

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
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetPreflightRequest",
  "unit": "export"
}
```

</details>
