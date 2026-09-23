# riverhog_protocol.TransformCapabilityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformcapabilitydocument:90d277fbc4 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-2277ae5e7d"></a>`signature`: `"\"(*, format: Literal['riverhog-transform-capability/v1'], id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[NonnegativeDecimal, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)], state: Literal['receiving', 'active'], principal_app: Annotated[str, MinLen(min_length=1), MaxLen(max_length=300)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], artifacts: riverhog_protocol.collection_workflow_transport.ArtifactReceivingSetDocument, token: Annotated[str, _PydanticGeneralMetadata(pattern='^rhc_[A-Za-z0-9_-]+$')]) -> None\""`

#### Validated model schema

<a id="s-071f2c7276"></a>

- <a id="s-f0a90a2c3d"></a>`type`: `"object"`
- <a id="s-cf1cfa6dd7"></a>`additionalProperties`: `false`
- <a id="s-4d842f019e"></a>`required`: `["format","id","claim_id","fence","audience","actions","state","principal_app","expires_at","artifacts","token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa27ce7040"></a>`actions` | yes | type="array"; items=(type="string"; enum=["read-inputs","write-output"]); minItems=1; oneOf=[(const=["read-inputs"]); (const=["read-inputs","write-output"])] |  |
| <a id="s-9717f29e89"></a>`artifacts` | yes | [ArtifactReceivingSetDocument](#s-12e03bb1bc) |  |
| <a id="s-0ea541ef21"></a>`audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$" |  |
| <a id="s-be290c9233"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5338f570fa"></a>`expires_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-ca39ae8afe"></a>`fence` | yes | [NonnegativeDecimal](#s-23fa00047a); ge=1 |  |
| <a id="s-71303d8f5c"></a>`format` | yes | type="string"; const="riverhog-transform-capability/v1" |  |
| <a id="s-1ecd9f3954"></a>`id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-2758f46f2a"></a>`principal_app` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-95ddc61ed1"></a>`state` | yes | type="string"; enum=["receiving","active"] |  |
| <a id="s-ed5f872a31"></a>`token` | yes | type="string"; pattern="^rhc_[A-Za-z0-9_-]+$" |  |

##### Definitions

- [ArtifactReceivingSetDocument](#s-12e03bb1bc)
- [ArtifactSetIdentityDocument](#s-176c5edaa6)
- [NonnegativeDecimal](#s-23fa00047a)

##### <a id="s-12e03bb1bc"></a>definition `ArtifactReceivingSetDocument`

- <a id="s-d7e66c6118"></a>`type`: `"object"`
- <a id="s-d2012fc7b7"></a>`additionalProperties`: `false`
- <a id="s-5db77e5248"></a>`required`: `["state","count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-56adc08a7d"></a>`count` | yes | [NonnegativeDecimal](#s-23fa00047a); ge=0 |  |
| <a id="s-37bf79b9c1"></a>`identity` | no | anyOf=[([ArtifactSetIdentityDocument](#s-176c5edaa6)); (type="null")]; default=null |  |
| <a id="s-415ecd6ca4"></a>`state` | yes | type="string"; enum=["receiving","sealed"] |  |
| <a id="s-a45e9e7b9d"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-23fa00047a); ge=0 |  |

##### <a id="s-176c5edaa6"></a>definition `ArtifactSetIdentityDocument`

- <a id="s-71f8983c39"></a>`type`: `"object"`
- <a id="s-d915ac7b5a"></a>`additionalProperties`: `false`
- <a id="s-6d96fb6181"></a>`required`: `["count","sha256","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cb880cf95e"></a>`count` | yes | [NonnegativeDecimal](#s-23fa00047a); ge=1 |  |
| <a id="s-313742681c"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c60be5b383"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-23fa00047a); ge=0 |  |

##### <a id="s-23fa00047a"></a>definition `NonnegativeDecimal`

- <a id="s-932eb4c290"></a>`type`: `"string"`
- <a id="s-8552a413dd"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-transformcapabilitydocument-getitem.md)
- [validate_capability](riverhog-protocol-transformcapabilitydocument-validate-capability.md)
- [get](riverhog-protocol-transformcapabilitydocument-get.md)

## Governing policies

- <a id="pa-eead9e1ca8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformCapabilityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e0b0c343a870dfc1da10d9dbc57098ae2bf2998581eefdc20c55670dab2f589 -->

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
    "signature": "\"(*, format: Literal['riverhog-transform-capability/v1'], id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[NonnegativeDecimal, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)], state: Literal['receiving', 'active'], principal_app: Annotated[str, MinLen(min_length=1), MaxLen(max_length=300)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], artifacts: riverhog_protocol.collection_workflow_transport.ArtifactReceivingSetDocument, token: Annotated[str, _PydanticGeneralMetadata(pattern='^rhc_[A-Za-z0-9_-]+$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "TransformCapabilityDocument",
  "unit": "export"
}
```

</details>
