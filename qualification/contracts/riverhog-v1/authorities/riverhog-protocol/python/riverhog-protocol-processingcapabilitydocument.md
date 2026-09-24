# riverhog_protocol.ProcessingCapabilityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingcapabilitydocument:c70a8d7f77 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3ae1e7f820"></a>
- <a id="s-634b554565"></a>`distribution`: `riverhog-protocol`
- <a id="s-d3efd31cd0"></a>`module`: `riverhog_protocol`
- <a id="s-d2ad129a6d"></a>`name`: `ProcessingCapabilityDocument`
- <a id="s-251e738f99"></a>`unit`: `export`

### Declared structure

- <a id="s-f3b21fe34e"></a>`kind`: `"class"`
- <a id="s-6d09b20ba5"></a>`signature`: `"\"(*, format: Literal['riverhog-processing-capability/v1'], id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[NonnegativeDecimal, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)], state: Literal['receiving', 'active'], principal_id: Annotated[PrincipalId, MaxLen(max_length=300)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], artifacts: riverhog_protocol.collection_workflow_transport.ArtifactReceivingSetDocument, token: Annotated[str, _PydanticGeneralMetadata(pattern='^rhc_[A-Za-z0-9_-]+$')]) -> None\""`

#### Validated model schema

<a id="s-80185d38b8"></a>

- <a id="s-2bb035749e"></a>`type`: `"object"`
- <a id="s-7ae263139e"></a>`additionalProperties`: `false`
- <a id="s-6c4c494c27"></a>`required`: `["format","id","claim_id","fence","audience","actions","state","principal_id","expires_at","artifacts","token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e1220a82d1"></a>`actions` | yes | type="array"; items=(type="string"; enum=["read-inputs","write-output"]); minItems=1; oneOf=[(const=["read-inputs"]); (const=["read-inputs","write-output"])] |  |
| <a id="s-a82727c0c0"></a>`artifacts` | yes | [ArtifactReceivingSetDocument](#s-1a551d3ec4) |  |
| <a id="s-66f5ae6534"></a>`audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$" |  |
| <a id="s-dc21d4b408"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c664bca7b4"></a>`expires_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-6665fc8382"></a>`fence` | yes | [NonnegativeDecimal](#s-b75be77288); ge=1 |  |
| <a id="s-d0bc5fb021"></a>`format` | yes | type="string"; const="riverhog-processing-capability/v1" |  |
| <a id="s-88e88e111a"></a>`id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-4ce0f41aa6"></a>`principal_id` | yes | [PrincipalId](#s-bf37cb2b76); maxLength=300 |  |
| <a id="s-e69319bb27"></a>`state` | yes | type="string"; enum=["receiving","active"] |  |
| <a id="s-895b9fa4aa"></a>`token` | yes | type="string"; pattern="^rhc_[A-Za-z0-9_-]+$" |  |

##### Definitions

- [ArtifactReceivingSetDocument](#s-1a551d3ec4)
- [ArtifactSetIdentityDocument](#s-a60bca71ce)
- [NonnegativeDecimal](#s-b75be77288)
- [PrincipalId](#s-bf37cb2b76)

##### <a id="s-1a551d3ec4"></a>definition `ArtifactReceivingSetDocument`

- <a id="s-10086a7ed2"></a>`type`: `"object"`
- <a id="s-742fed14cb"></a>`additionalProperties`: `false`
- <a id="s-65e4b14527"></a>`required`: `["state","count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d3da821e37"></a>`count` | yes | [NonnegativeDecimal](#s-b75be77288); ge=0 |  |
| <a id="s-a45edacbc3"></a>`identity` | no | anyOf=[([ArtifactSetIdentityDocument](#s-a60bca71ce)); (type="null")]; default=null |  |
| <a id="s-92e9aec5f2"></a>`state` | yes | type="string"; enum=["receiving","sealed"] |  |
| <a id="s-c9a8bcc7db"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-b75be77288); ge=0 |  |

##### <a id="s-a60bca71ce"></a>definition `ArtifactSetIdentityDocument`

- <a id="s-0cf73620fa"></a>`type`: `"object"`
- <a id="s-9effa587cf"></a>`additionalProperties`: `false`
- <a id="s-9aa39c1833"></a>`required`: `["count","sha256","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3e060d3b85"></a>`count` | yes | [NonnegativeDecimal](#s-b75be77288); ge=1 |  |
| <a id="s-9f71210f7d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4aa40cd873"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-b75be77288); ge=0 |  |

##### <a id="s-b75be77288"></a>definition `NonnegativeDecimal`

- <a id="s-8529514802"></a>`type`: `"string"`
- <a id="s-5cb69fb826"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-bf37cb2b76"></a>definition `PrincipalId`

- <a id="s-9e8b92a8e2"></a>`type`: `"string"`
- <a id="s-095baa3ee1"></a>`pattern`: `"^(?:[a-z0-9]+(?:-[a-z0-9]+)*\|claim:[0-9a-f]{64}\|processing:[0-9a-f]{64})$"`

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingcapabilitydocument-getitem.md)
- [validate_capability](riverhog-protocol-processingcapabilitydocument-validate-capability.md)
- [get](riverhog-protocol-processingcapabilitydocument-get.md)

## Governing policies

- <a id="pa-f2f53ed612"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingCapabilityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ee848a4f42958e809d2897e78879b6060c5ac1b60a1ac032ff8560cb3cb2d3c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactReceivingSetDocument": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
            },
            "identity": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ArtifactSetIdentityDocument"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "state": {
              "enum": [
                "receiving",
                "sealed"
              ],
              "type": "string"
            },
            "total_bytes": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
            }
          },
          "required": [
            "state",
            "count",
            "total_bytes"
          ],
          "type": "object"
        },
        "ArtifactSetIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
            }
          },
          "required": [
            "count",
            "sha256",
            "total_bytes"
          ],
          "type": "object"
        },
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "PrincipalId": {
          "pattern": "^(?:[a-z0-9]+(?:-[a-z0-9]+)*|claim:[0-9a-f]{64}|processing:[0-9a-f]{64})$",
          "type": "string"
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
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "format": {
          "const": "riverhog-processing-capability/v1",
          "type": "string"
        },
        "id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "principal_id": {
          "$ref": "#/$defs/PrincipalId",
          "maxLength": 300
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
        "principal_id",
        "expires_at",
        "artifacts",
        "token"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-processing-capability/v1'], id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[NonnegativeDecimal, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)], state: Literal['receiving', 'active'], principal_id: Annotated[PrincipalId, MaxLen(max_length=300)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], artifacts: riverhog_protocol.collection_workflow_transport.ArtifactReceivingSetDocument, token: Annotated[str, _PydanticGeneralMetadata(pattern='^rhc_[A-Za-z0-9_-]+$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingCapabilityDocument",
  "unit": "export"
}
```

</details>
