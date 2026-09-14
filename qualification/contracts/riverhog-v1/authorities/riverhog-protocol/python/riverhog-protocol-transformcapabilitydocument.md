# riverhog_protocol.TransformCapabilityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformcapabilitydocument:90d277fbc4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-80113a1f97"></a>
- <a id="s-7236ba0473"></a>`distribution`: `riverhog-protocol`
- <a id="s-03fd69d195"></a>`module`: `riverhog_protocol`
- <a id="s-e9531ea968"></a>`name`: `TransformCapabilityDocument`
- <a id="s-8442e14233"></a>`unit`: `export`

### Declared structure

- <a id="s-c2eaf2e993"></a>`kind`: `"class"`
- <a id="s-2277ae5e7d"></a>`signature`: `"\"(*, format: Literal['riverhog-transform-capability/v1'], id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[int, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)], state: Literal['receiving', 'active'], principal_app: Annotated[str, MinLen(min_length=1), MaxLen(max_length=300)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], artifacts: riverhog_protocol.collection_workflow_transport.ArtifactReceivingSetDocument, token: Annotated[str, _PydanticGeneralMetadata(pattern='^rhc_[A-Za-z0-9_-]+$')]) -> None\""`

#### Validated model schema

<a id="s-071f2c7276"></a>
- <a id="s-f3f3895318"></a>`title`: TransformCapabilityDocument
- <a id="s-f0a90a2c3d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa27ce7040"></a>`actions` | yes | type="array"; minItems=1; items=(type="string"; enum=["read-inputs","write-output"]); oneOf=const=["read-inputs"] \| const=["read-inputs","write-output"] |  |
| <a id="s-9717f29e89"></a>`artifacts` | yes | #/$defs/ArtifactReceivingSetDocument |  |
| <a id="s-0ea541ef21"></a>`audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$" |  |
| <a id="s-be290c9233"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5338f570fa"></a>`expires_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| <a id="s-ca39ae8afe"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-71303d8f5c"></a>`format` | yes | type="string"; const="riverhog-transform-capability/v1" |  |
| <a id="s-1ecd9f3954"></a>`id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-2758f46f2a"></a>`principal_app` | yes | type="string"; minLength=1; maxLength=300 |  |
| <a id="s-95ddc61ed1"></a>`state` | yes | type="string"; enum=["receiving","active"] |  |
| <a id="s-ed5f872a31"></a>`token` | yes | type="string"; pattern="^rhc_[A-Za-z0-9_-]+$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-12e03bb1bc"></a>`ArtifactReceivingSetDocument` | type="object"; fields=`authority`, `count`, `state`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-6a2b19199e"></a>`ArtifactSetAuthorityDocument` | type="object"; fields=`count`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.TransformCapabilityDocument.validate_capability](riverhog-protocol-transformcapabilitydocument-validate-capability.md)

## Governing policies

- <a id="pa-eead9e1ca8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformCapabilityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ee128d8f17ec4e3d7b6bbefe5e565e113d30fd826a9b41ca04d6a8be83d48547 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactReceivingSetDocument": {
          "additionalProperties": false,
          "properties": {
            "authority": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ArtifactSetAuthorityDocument"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "count": {
              "minimum": 0,
              "title": "Count",
              "type": "integer"
            },
            "state": {
              "enum": [
                "receiving",
                "sealed"
              ],
              "title": "State",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "title": "Total Bytes",
              "type": "integer"
            }
          },
          "required": [
            "state",
            "count",
            "total_bytes"
          ],
          "title": "ArtifactReceivingSetDocument",
          "type": "object"
        },
        "ArtifactSetAuthorityDocument": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "title": "Count",
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "title": "Total Bytes",
              "type": "integer"
            }
          },
          "required": [
            "count",
            "sha256",
            "total_bytes"
          ],
          "title": "ArtifactSetAuthorityDocument",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "actions": {
          "items": {
            "enum": [
              "read-inputs",
              "write-output"
            ],
            "type": "string"
          },
          "minItems": 1,
          "oneOf": [
            {
              "const": [
                "read-inputs"
              ]
            },
            {
              "const": [
                "read-inputs",
                "write-output"
              ]
            }
          ],
          "title": "Actions",
          "type": "array"
        },
        "artifacts": {
          "$ref": "#/$defs/ArtifactReceivingSetDocument"
        },
        "audience": {
          "pattern": "^[a-z0-9][a-z0-9._:/-]{0,299}$",
          "title": "Audience",
          "type": "string"
        },
        "claim_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Claim Id",
          "type": "string"
        },
        "expires_at": {
          "maxLength": 64,
          "minLength": 1,
          "title": "Expires At",
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "title": "Fence",
          "type": "integer"
        },
        "format": {
          "const": "riverhog-transform-capability/v1",
          "title": "Format",
          "type": "string"
        },
        "id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Id",
          "type": "string"
        },
        "principal_app": {
          "maxLength": 300,
          "minLength": 1,
          "title": "Principal App",
          "type": "string"
        },
        "state": {
          "enum": [
            "receiving",
            "active"
          ],
          "title": "State",
          "type": "string"
        },
        "token": {
          "pattern": "^rhc_[A-Za-z0-9_-]+$",
          "title": "Token",
          "type": "string"
        }
      },
      "required": [
        "format",
        "id",
        "claim_id",
        "fence",
        "audience",
        "actions",
        "state",
        "principal_app",
        "expires_at",
        "artifacts",
        "token"
      ],
      "title": "TransformCapabilityDocument",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-transform-capability/v1'], id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[int, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)], state: Literal['receiving', 'active'], principal_app: Annotated[str, MinLen(min_length=1), MaxLen(max_length=300)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], artifacts: riverhog_protocol.collection_workflow_transport.ArtifactReceivingSetDocument, token: Annotated[str, _PydanticGeneralMetadata(pattern='^rhc_[A-Za-z0-9_-]+$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "TransformCapabilityDocument",
  "unit": "export"
}
```
