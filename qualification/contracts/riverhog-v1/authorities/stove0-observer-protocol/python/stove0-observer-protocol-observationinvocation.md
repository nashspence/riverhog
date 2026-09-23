# stove0_observer_protocol.ObservationInvocation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationinvocation:665ac541d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0a9c0d3b70"></a>
- <a id="s-49ec3511db"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-bb267f858b"></a>`module`: `stove0_observer_protocol`
- <a id="s-a283eb5adf"></a>`name`: `ObservationInvocation`
- <a id="s-c66a337eab"></a>`unit`: `export`

### Declared structure

- <a id="s-c2d8a4a699"></a>`kind`: `"class"`
- <a id="s-97ee3b53e9"></a>`signature`: `"'(*, request: stove0_protocol.models.ObservationRequest, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], runtime: stove0_protocol.models.ObserverRuntimeAuthority) -> None'"`

#### Validated model schema

<a id="s-0296a30214"></a>

- <a id="s-affc330a90"></a>`type`: `"object"`
- <a id="s-48fe083073"></a>`additionalProperties`: `false`
- <a id="s-5bcf913528"></a>`required`: `["request","claim_id","fence","runtime"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a7e3975231"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-9c8d53db83"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-80b6631af8"></a>`request` | yes | [ObservationRequest](#s-957e246dd2) |  |
| <a id="s-c6b96ab6fd"></a>`runtime` | yes | [ObserverRuntimeAuthority](#s-9875058c01) |  |

##### Definitions

- [ArtifactSubject](#s-a6a9f9a8cd)
- [CollectionId](#s-9d6c2f9f38)
- [CollectionRootRef](#s-52bea440ad)
- [DeclaredWorkspaceProtection](#s-a41011dced)
- [JsonValue](#s-3c9822b86f)
- [ObservationRequest](#s-957e246dd2)
- [ObserverRuntimeAuthority](#s-9875058c01)

##### <a id="s-a6a9f9a8cd"></a>definition `ArtifactSubject`

- <a id="s-9c84c3e137"></a>`type`: `"object"`
- <a id="s-1d617e5733"></a>`additionalProperties`: `false`
- <a id="s-6ad0f793b8"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ddfa36e499"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-5d46afcfe6"></a>`collection` | yes | [CollectionRootRef](#s-52bea440ad) |  |
| <a id="s-90dc0592f4"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-77f1f69ae9"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-f9983989ac"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-b27a53db28"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cc743505f2"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9d6c2f9f38"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-80c9dcb1ab"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-39706d8fa5"></a>2 | not=(const="0") |

##### <a id="s-52bea440ad"></a>definition `CollectionRootRef`

- <a id="s-8c46896eac"></a>`type`: `"object"`
- <a id="s-7d0edb13aa"></a>`additionalProperties`: `false`
- <a id="s-3f6116917b"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-96ca42b955"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-af16933782"></a>`collection_id` | yes | [CollectionId](#s-9d6c2f9f38) |  |
| <a id="s-c6e424916a"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a41011dced"></a>definition `DeclaredWorkspaceProtection`

- <a id="s-495d199b0c"></a>`type`: `"string"`
- <a id="s-a15f7d2536"></a>`enum`: `["encrypted-at-rest","memory-backed"]`

##### <a id="s-3c9822b86f"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-957e246dd2"></a>definition `ObservationRequest`

- <a id="s-4b396e5c5d"></a>`type`: `"object"`
- <a id="s-408960f23a"></a>`additionalProperties`: `false`
- <a id="s-3b20cfb686"></a>`required`: `["work_id","observer_registration_id","observer_descriptor_sha256","observer_contract_id","observer_contract_sha256","subjects","request_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35bb6d3df8"></a>`format` | no | type="string"; const="stove0-observation-request/v1"; default="stove0-observation-request/v1" |  |
| <a id="s-8601a5545d"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-b44b3e6c9a"></a>`observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1893fa2f6a"></a>`observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fa5ae44d1c"></a>`observer_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c8dcc57703"></a>`observer_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-5104ee09c8"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-3c9822b86f)) |  |
| <a id="s-a0f8323c58"></a>`request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ab17c5e2bb"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-a431ac1d6a"></a>`subjects` | yes | type="array"; items=([ArtifactSubject](#s-a6a9f9a8cd)); minItems=1 |  |
| <a id="s-1f93a88c70"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |
| <a id="s-e803ce8a24"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-9875058c01"></a>definition `ObserverRuntimeAuthority`

- <a id="s-5fde14d9e7"></a>`type`: `"object"`
- <a id="s-fdea5b7ab6"></a>`additionalProperties`: `false`
- <a id="s-37489ed37a"></a>`required`: `["riverhog_base_url","capability_token","declared_workspace_protection"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0cceaee2c7"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-924a8acb24"></a>`capability_token` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-eb28039b80"></a>`declared_workspace_protection` | yes | [DeclaredWorkspaceProtection](#s-a41011dced) |  |
| <a id="s-93b095a72f"></a>`riverhog_base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-332a21d897"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

## Maintained corroboration

### Related interface records

- [canonical_claim_id](stove0-observer-protocol-observationinvocation-canonical-claim-id.md)

## Governing policies

- <a id="pa-5c68c4d5d8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationInvocation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 161739844fb24612fb10b0608bc1de6e99e766265f321d1d521f12269032a2bc -->

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
        "DeclaredWorkspaceProtection": {
          "enum": [
            "encrypted-at-rest",
            "memory-backed"
          ],
          "type": "string"
        },
        "JsonValue": {},
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
          "$ref": "#/$defs/ObservationRequest"
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
    "signature": "'(*, request: stove0_protocol.models.ObservationRequest, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], runtime: stove0_protocol.models.ObserverRuntimeAuthority) -> None'"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationInvocation",
  "unit": "export"
}
```

</details>
