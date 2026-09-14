# stove0_observer_protocol.ObservationInvocation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationinvocation:665ac541d4 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-0a325cb1e8"></a>`title`: ObservationInvocation
- <a id="s-03135d163f"></a>`description`: Fence-bound invocation authority excluded from semantic request identity.
- <a id="s-affc330a90"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a7e3975231"></a>`claim_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-9c8d53db83"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-80b6631af8"></a>`request` | yes | #/$defs/ObservationRequest |  |
| <a id="s-c6b96ab6fd"></a>`runtime` | yes | #/$defs/ObserverRuntimeAuthority |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-a6a9f9a8cd"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-9d6c2f9f38"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-52bea440ad"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-3c9822b86f"></a>`JsonValue` | empty object |
| <a id="s-957e246dd2"></a>`ObservationRequest` | type="object"; fields=`format`, `maximum_result_bytes`, `observer_contract_id`, `observer_contract_sha256`, `observer_descriptor_sha256`, `observer_registration_id`, `options`, `request_id`, `retrieval_policy`, `subjects`, `timeout_seconds`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-9875058c01"></a>`ObserverRuntimeAuthority` | type="object"; fields=`allow_insecure_http`, `capability_token`, `riverhog_base_url`, `transport`, `workspace_assurance`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObservationInvocation.canonical_claim_id](stove0-observer-protocol-observationinvocation-canonical-claim-id.md)

## Governing policies

- <a id="pa-5c68c4d5d8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationInvocation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f422b277732218b57e78a302c8ac913c901e8b19eb02428070e47ed9dbaa8d6 -->

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
              "title": "Bytes",
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
              "title": "Id",
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
              "default": null,
              "title": "Media Type"
            },
            "path": {
              "maxLength": 4096,
              "minLength": 1,
              "title": "Path",
              "type": "string"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Role",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
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
          "title": "ArtifactSubject",
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
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "title": "CollectionRootRef",
          "type": "object"
        },
        "JsonValue": {},
        "ObservationRequest": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-observation-request/v1",
              "default": "stove0-observation-request/v1",
              "title": "Format",
              "type": "string"
            },
            "maximum_result_bytes": {
              "default": 1048576,
              "maximum": 67108864,
              "minimum": 1,
              "title": "Maximum Result Bytes",
              "type": "integer"
            },
            "observer_contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Observer Contract Id",
              "type": "string"
            },
            "observer_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Observer Contract Sha256",
              "type": "string"
            },
            "observer_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Observer Descriptor Sha256",
              "type": "string"
            },
            "observer_registration_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
              "title": "Observer Registration Id",
              "type": "string"
            },
            "options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "title": "Options",
              "type": "object"
            },
            "request_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Request Id",
              "type": "string"
            },
            "retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "title": "Retrieval Policy",
              "type": "string"
            },
            "subjects": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
              "title": "Subjects",
              "type": "array"
            },
            "timeout_seconds": {
              "default": 300,
              "maximum": 86400,
              "minimum": 1,
              "title": "Timeout Seconds",
              "type": "integer"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Work Id",
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
          "title": "ObservationRequest",
          "type": "object"
        },
        "ObserverRuntimeAuthority": {
          "additionalProperties": false,
          "description": "Secret-bearing invocation material excluded from durable request identity.",
          "properties": {
            "allow_insecure_http": {
              "default": false,
              "title": "Allow Insecure Http",
              "type": "boolean"
            },
            "capability_token": {
              "maxLength": 4096,
              "minLength": 1,
              "title": "Capability Token",
              "type": "string"
            },
            "riverhog_base_url": {
              "maxLength": 2048,
              "minLength": 1,
              "title": "Riverhog Base Url",
              "type": "string"
            },
            "transport": {
              "const": "riverhog-capability/v1",
              "default": "riverhog-capability/v1",
              "title": "Transport",
              "type": "string"
            },
            "workspace_assurance": {
              "enum": [
                "encrypted",
                "ephemeral"
              ],
              "title": "Workspace Assurance",
              "type": "string"
            }
          },
          "required": [
            "riverhog_base_url",
            "capability_token",
            "workspace_assurance"
          ],
          "title": "ObserverRuntimeAuthority",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "description": "Fence-bound invocation authority excluded from semantic request identity.",
      "properties": {
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Claim Id",
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "title": "Fence",
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
      "title": "ObservationInvocation",
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
