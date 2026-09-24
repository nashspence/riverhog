# stove0_observer_protocol.ContentObservationInvocation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-2d04411c2a:ec8a17342d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-701749f2d6"></a>
- <a id="s-c9befd9af6"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-eed8c35327"></a>`module`: `stove0_observer_protocol`
- <a id="s-ba9f92d08c"></a>`name`: `ContentObservationInvocation`
- <a id="s-dd885a1767"></a>`unit`: `export`

### Declared structure

- <a id="s-958dc95b14"></a>`kind`: `"class"`
- <a id="s-5b8b37fbf0"></a>`signature`: `"'(*, request: stove0_protocol.models.ContentObservationRequest, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], runtime: stove0_protocol.models.ObserverRuntimeAuthority) -> None'"`

#### Validated model schema

<a id="s-1a65453807"></a>

- <a id="s-88207e749d"></a>`type`: `"object"`
- <a id="s-673179cbd0"></a>`additionalProperties`: `false`
- <a id="s-ad285d0aaa"></a>`required`: `["request","claim_id","fence","runtime"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b0915909c3"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-c92bf6f221"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-a03875c231"></a>`request` | yes | [ContentObservationRequest](#s-ea45b782aa) |  |
| <a id="s-709f7d0cca"></a>`runtime` | yes | [ObserverRuntimeAuthority](#s-5425c1d762) |  |

##### Definitions

- [CollectionId](#s-76400c1137)
- [CollectionRootIdentityRef](#s-abeb4693f2)
- [ContentObservationRequest](#s-ea45b782aa)
- [DeclaredWorkspaceProtection](#s-177f0aca9a)
- [JsonValue](#s-bc767ada5d)
- [NonnegativeDecimal](#s-60c6e1fe4a)
- [ObserverRuntimeAuthority](#s-5425c1d762)
- [WorkArtifactSubject](#s-158516dea6)

##### <a id="s-76400c1137"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-07e00a56eb"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-b8e1223506"></a>2 | not=(const="0") |

##### <a id="s-abeb4693f2"></a>definition `CollectionRootIdentityRef`

- <a id="s-e7540c7830"></a>`type`: `"object"`
- <a id="s-265c3e2f8c"></a>`additionalProperties`: `false`
- <a id="s-46eeae4416"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-70c594ef8b"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-110d6c48d8"></a>`collection_id` | yes | [CollectionId](#s-76400c1137) |  |
| <a id="s-c0a7566eed"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ea45b782aa"></a>definition `ContentObservationRequest`

- <a id="s-ee1fb9d287"></a>`type`: `"object"`
- <a id="s-91f0467d99"></a>`additionalProperties`: `false`
- <a id="s-f27926d1a1"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8952057179"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-f0e94d61d3"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-e65892ab69"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-58ea4f72fb"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e568ad227e"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a8c688e97d"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-8106ec8f7c"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-bc767ada5d)) |  |
| <a id="s-0bd8ac9bc4"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-84c17879fa"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-73ef53e8cc"></a>`subjects` | yes | type="array"; items=([WorkArtifactSubject](#s-158516dea6)); minItems=1 |  |
| <a id="s-bfae56cf6d"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-9743300423"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-177f0aca9a"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-7526009135"></a>`type`: `"string"`
- <a id="s-e0853fefad"></a>`enum`: `["encrypted-at-rest","memory-backed"]`

##### <a id="s-bc767ada5d"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-60c6e1fe4a"></a>definition `NonnegativeDecimal`

- <a id="s-9ae8cb52ed"></a>`type`: `"string"`
- <a id="s-2e81239ccf"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-5425c1d762"></a>definition `ObserverRuntimeAuthority`

- <a id="s-48ea02bb1a"></a>`type`: `"object"`
- <a id="s-1f88946249"></a>`additionalProperties`: `false`
- <a id="s-17b91014b6"></a>`required`: `["riverhog_base_url","capability_token","declared_workspace_protection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6f9feadb6"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-0adc6ebd60"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-fbff095c86"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-177f0aca9a) |  |
| <a id="s-1a3135652c"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-c19b278988"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### <a id="s-158516dea6"></a>definition `WorkArtifactSubject`

- <a id="s-37d95e7fd5"></a>`type`: `"object"`
- <a id="s-f3f6e33349"></a>`additionalProperties`: `false`
- <a id="s-4619e17676"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-51ae5f4a02"></a>`bytes` | yes | [NonnegativeDecimal](#s-60c6e1fe4a); ge=0 |  |
| <a id="s-cc44fa1ac2"></a>`collection` | yes | [CollectionRootIdentityRef](#s-abeb4693f2) |  |
| <a id="s-bb92ed8021"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-8863f4d78f"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-73a752786d"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-c337beda85"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9850831206"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_claim_id](stove0-observer-protocol-contentobservationinvocation-canonical-claim-id.md)

## Governing policies

- <a id="pa-069d1c7e0e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationInvocation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84086a7ec6c98bc75af217d7ef101576566fc3abe2e112bde9a07d140556f8a2 -->

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
        "ContentObservationRequest": {
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
                "$ref": "#/$defs/WorkArtifactSubject"
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
        "DeclaredWorkspaceProtection": {
          "enum": [
            "encrypted-at-rest",
            "memory-backed"
          ],
          "type": "string"
        },
        "JsonValue": {},
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "ObserverRuntimeAuthority": {
          "additionalProperties": false,
          "properties": {
            "allow_insecure_http": {
              "default": false,
              "type": "boolean"
            },
            "capability_token": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "declared_workspace_protection": {
              "$ref": "#/$defs/DeclaredWorkspaceProtection"
            },
            "riverhog_base_url": {
              "maxLength": 2048,
              "minLength": 1,
              "type": "string"
            },
            "transport": {
              "const": "riverhog-capability/v1",
              "default": "riverhog-capability/v1",
              "type": "string"
            }
          },
          "required": [
            "riverhog_base_url",
            "capability_token",
            "declared_workspace_protection"
          ],
          "type": "object"
        },
        "WorkArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
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
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "type": "integer"
        },
        "request": {
          "$ref": "#/$defs/ContentObservationRequest"
        },
        "runtime": {
          "$ref": "#/$defs/ObserverRuntimeAuthority"
        }
      },
      "required": [
        "request",
        "claim_id",
        "fence",
        "runtime"
      ],
      "type": "object"
    },
    "signature": "'(*, request: stove0_protocol.models.ContentObservationRequest, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], runtime: stove0_protocol.models.ObserverRuntimeAuthority) -> None'"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ContentObservationInvocation",
  "unit": "export"
}
```

</details>
