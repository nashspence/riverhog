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

- <a id="s-f0a90a2c3d"></a>`type`: `"object"`
- <a id="s-cf1cfa6dd7"></a>`additionalProperties`: `false`
- <a id="s-4d842f019e"></a>`required`: `["format","id","claim_id","fence","audience","actions","state","principal_app","expires_at","artifacts","token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa27ce7040"></a>`actions` | yes | type="array"; items=(type="string"; enum=["read-inputs","write-output"]); minItems=1; oneOf=(const=["read-inputs"]) \| (const=["read-inputs","write-output"]) |  |
| <a id="s-9717f29e89"></a>`artifacts` | yes | [ArtifactReceivingSetDocument](#s-12e03bb1bc) |  |
| <a id="s-0ea541ef21"></a>`audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$" |  |
| <a id="s-be290c9233"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5338f570fa"></a>`expires_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-ca39ae8afe"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-71303d8f5c"></a>`format` | yes | type="string"; const="riverhog-transform-capability/v1" |  |
| <a id="s-1ecd9f3954"></a>`id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-2758f46f2a"></a>`principal_app` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-95ddc61ed1"></a>`state` | yes | type="string"; enum=["receiving","active"] |  |
| <a id="s-ed5f872a31"></a>`token` | yes | type="string"; pattern="^rhc_[A-Za-z0-9_-]+$" |  |

##### Definitions

- [ArtifactReceivingSetDocument](#s-12e03bb1bc)
- [ArtifactSetAuthorityDocument](#s-6a2b19199e)

##### <a id="s-12e03bb1bc"></a>definition `ArtifactReceivingSetDocument`

- <a id="s-d7e66c6118"></a>`type`: `"object"`
- <a id="s-d2012fc7b7"></a>`additionalProperties`: `false`
- <a id="s-5db77e5248"></a>`required`: `["state","count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e7fe48037"></a>`authority` | no | anyOf=([ArtifactSetAuthorityDocument](#s-6a2b19199e)) \| (type="null"); default=null |  |
| <a id="s-56adc08a7d"></a>`count` | yes | type="integer"; minimum=0 |  |
| <a id="s-415ecd6ca4"></a>`state` | yes | type="string"; enum=["receiving","sealed"] |  |
| <a id="s-a45e9e7b9d"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-6a2b19199e"></a>definition `ArtifactSetAuthorityDocument`

- <a id="s-fca2b14728"></a>`type`: `"object"`
- <a id="s-b07d92120a"></a>`additionalProperties`: `false`
- <a id="s-4743c36528"></a>`required`: `["count","sha256","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee5ab19b33"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-640267e39e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-247e9891d1"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-transformcapabilitydocument-getitem.md)
- [validate_capability](riverhog-protocol-transformcapabilitydocument-validate-capability.md)
- [get](riverhog-protocol-transformcapabilitydocument-get.md)

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

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c21fdc91dd382badc27dc1a5476b3153c9d3e511f0faeae42e333ef53689aa0 -->

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
              "type": "integer"
            },
            "state": {
              "enum": [
                "receiving",
                "sealed"
              ],
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "state",
            "count",
            "total_bytes"
          ],
          "type": "object"
        },
        "ArtifactSetAuthorityDocument": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "count",
            "sha256",
            "total_bytes"
          ],
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
          "type": "array"
        },
        "artifacts": {
          "$ref": "#/$defs/ArtifactReceivingSetDocument"
        },
        "audience": {
          "pattern": "^[a-z0-9][a-z0-9._:/-]{0,299}$",
          "type": "string"
        },
        "claim_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "expires_at": {
          "maxLength": 64,
          "minLength": 1,
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "type": "integer"
        },
        "format": {
          "const": "riverhog-transform-capability/v1",
          "type": "string"
        },
        "id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "principal_app": {
          "maxLength": 300,
          "minLength": 1,
          "type": "string"
        },
        "state": {
          "enum": [
            "receiving",
            "active"
          ],
          "type": "string"
        },
        "token": {
          "pattern": "^rhc_[A-Za-z0-9_-]+$",
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

</details>
