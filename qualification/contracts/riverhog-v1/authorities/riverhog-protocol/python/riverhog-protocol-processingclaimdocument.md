# riverhog_protocol.ProcessingClaimDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimdocument:26bbd50563 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75863d0da4"></a>
- <a id="s-3a9e8ca942"></a>`distribution`: `riverhog-protocol`
- <a id="s-0c651f9b71"></a>`module`: `riverhog_protocol`
- <a id="s-17efc6ff7d"></a>`name`: `ProcessingClaimDocument`
- <a id="s-1118c39165"></a>`unit`: `export`

### Declared structure

- <a id="s-fca0c2e8a3"></a>`kind`: `"class"`
- <a id="s-9ec17805fb"></a>`signature`: `"\"(*, format: Literal['riverhog-processing-claim/v1'], id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], consumer: riverhog_protocol.collection_workflow_transport.ProcessingClaimConsumerDocument, purpose: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['active', 'settled', 'retiring', 'abandoned', 'released'], fence: Annotated[int, Ge(ge=1)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], created_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], settled_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandoned_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandonment_reason: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, released_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, output_collection_id: CollectionId \| None = None, work_document: dict[str, typing.Any], work_document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: riverhog_protocol.collection_workflow_transport.ReceivingSetDocument, plan: riverhog_protocol.collection_workflow_transport.ProcessingClaimPlanDocument \| None = None, outcomes: riverhog_protocol.collection_workflow_transport.OutcomeSetDocument, outcome_settlement: riverhog_protocol.collection_workflow_transport.ProcessingClaimOutcomeSettlementDocument \| None = None) -> None\""`

#### Validated model schema

<a id="s-75d5c675f7"></a>

- <a id="s-adb9d9f057"></a>`type`: `"object"`
- <a id="s-898ac99a28"></a>`additionalProperties`: `false`
- <a id="s-493e4b6f80"></a>`required`: `["format","id","work_id","consumer","purpose","state","fence","expires_at","created_at","updated_at","work_document","work_document_sha256","inputs","outcomes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed124be02c"></a>`abandoned_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-361ff7cea9"></a>`abandonment_reason` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; default=null |  |
| <a id="s-53711020c5"></a>`consumer` | yes | [ProcessingClaimConsumerDocument](#s-d6d970f005) |  |
| <a id="s-fedd7a0acb"></a>`created_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-273f7eec10"></a>`expires_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-cfc44f9e08"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-bebb4904f4"></a>`format` | yes | type="string"; const="riverhog-processing-claim/v1" |  |
| <a id="s-90373974ca"></a>`id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a50c2084a7"></a>`inputs` | yes | [ReceivingSetDocument](#s-9dd4ce27cb) |  |
| <a id="s-dd6da4834d"></a>`outcome_settlement` | no | anyOf=[([ProcessingClaimOutcomeSettlementDocument](#s-e2510804b4)); (type="null")]; default=null |  |
| <a id="s-6d065e28b7"></a>`outcomes` | yes | [OutcomeSetDocument](#s-f7d5d1ec59) |  |
| <a id="s-4fd9d45af3"></a>`output_collection_id` | no | anyOf=[([CollectionId](#s-896ee01c47)); (type="null")]; default=null |  |
| <a id="s-a4ffc938e5"></a>`plan` | no | anyOf=[([ProcessingClaimPlanDocument](#s-c3d365e968)); (type="null")]; default=null |  |
| <a id="s-c744090071"></a>`purpose` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-d1e10a80ab"></a>`released_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-17bea566dd"></a>`settled_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-ef952ef113"></a>`state` | yes | type="string"; enum=["active","settled","retiring","abandoned","released"] |  |
| <a id="s-34b72d57f0"></a>`updated_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-7b3a390533"></a>`work_document` | yes | type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4194304; x-riverhog-extent={"policy":"contract_max","reason":"bounded-work-document-envelope"} |  |
| <a id="s-e58e8a592c"></a>`work_document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9d8834edff"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-b23147f06d"></a>1 | properties={state: (enum=["settled","retiring","released"])} | properties={settled_at: (type="string")}; required=["settled_at"] | properties={settled_at: (type="null")} |
| <a id="s-bc5298df01"></a>2 | properties={state: (const="abandoned")} | properties={abandoned_at: (type="string"); abandonment_reason: (type="string")}; required=["abandoned_at","abandonment_reason"] | properties={abandoned_at: (type="null"); abandonment_reason: (type="null")} |
| <a id="s-6c6ec42cc8"></a>3 | properties={state: (const="released")} | properties={released_at: (type="string")}; required=["released_at"] | properties={released_at: (type="null")} |

##### Definitions

- [ArtifactSetAuthorityDocument](#s-7c0e8d7455)
- [CollectionId](#s-896ee01c47)
- [ExactSetAuthorityDocument](#s-52c5516811)
- [OperationIdentityDocument](#s-e96e2dd600)
- [OutcomeSetDocument](#s-f7d5d1ec59)
- [ProcessingClaimConsumerDocument](#s-d6d970f005)
- [ProcessingClaimOutcomeSettlementDocument](#s-e2510804b4)
- [ProcessingClaimPlanDocument](#s-c3d365e968)
- [ReceivingSetDocument](#s-9dd4ce27cb)

##### <a id="s-7c0e8d7455"></a>definition `ArtifactSetAuthorityDocument`

- <a id="s-a62e3e9971"></a>`type`: `"object"`
- <a id="s-f6e3a66bfe"></a>`additionalProperties`: `false`
- <a id="s-ed403c32b4"></a>`required`: `["count","sha256","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3a29e5c523"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-c8a3da8ac2"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-907f8811ba"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-896ee01c47"></a>definition `CollectionId`

- <a id="s-ae282ddafb"></a>`type`: `"integer"`
- <a id="s-19ccf4f86d"></a>`minimum`: `1`

##### <a id="s-52c5516811"></a>definition `ExactSetAuthorityDocument`

- <a id="s-7bc43fdf9b"></a>`type`: `"object"`
- <a id="s-7cd9bcdefa"></a>`additionalProperties`: `false`
- <a id="s-bd59362e6e"></a>`required`: `["count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6ccadb9602"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-7840deac21"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e96e2dd600"></a>definition `OperationIdentityDocument`

- <a id="s-2e42d0751b"></a>`type`: `"object"`
- <a id="s-0c7c33165a"></a>`additionalProperties`: `false`
- <a id="s-f8842a3f0d"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a9a14feb41"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d124433996"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-f7d5d1ec59"></a>definition `OutcomeSetDocument`

- <a id="s-a2b1ffa89b"></a>`type`: `"object"`
- <a id="s-a912661e25"></a>`additionalProperties`: `false`
- <a id="s-add620372d"></a>`required`: `["state","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9f2c57fac8"></a>`authority` | no | anyOf=[([ExactSetAuthorityDocument](#s-52c5516811)); (type="null")]; default=null |  |
| <a id="s-6f579e73cf"></a>`count` | yes | type="integer"; minimum=0 |  |
| <a id="s-5d8d8f1e11"></a>`failure` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; default=null |  |
| <a id="s-67d870ae70"></a>`state` | yes | type="string"; enum=["receiving","sealing","sealed","failed"] |  |

##### <a id="s-d6d970f005"></a>definition `ProcessingClaimConsumerDocument`

- <a id="s-c4f5f4ad87"></a>`type`: `"object"`
- <a id="s-9eb7eaac88"></a>`additionalProperties`: `false`
- <a id="s-a9dc1376a6"></a>`required`: `["app"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2df569eeb1"></a>`app` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3167ec076b"></a>`key_id` | no | anyOf=[(type="string"; maxLength=300; minLength=1); (type="null")]; default=null |  |

##### <a id="s-e2510804b4"></a>definition `ProcessingClaimOutcomeSettlementDocument`

- <a id="s-ac0a622d02"></a>`type`: `"object"`
- <a id="s-102f7f1d18"></a>`additionalProperties`: `false`
- `if`: [See definition `ProcessingClaimOutcomeSettlementDocument` · `if`](#s-c359acdbc4)
- <a id="s-d93d68c6cc"></a>`required`: `["outcomes","retirement_policy","retirement_grace_seconds"]`
- `then`: [See definition `ProcessingClaimOutcomeSettlementDocument` · `then`](#s-e65195034c)

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-36d27ecd02"></a>`outcomes` | yes | [ExactSetAuthorityDocument](#s-52c5516811) |  |
| <a id="s-4410371a95"></a>`retirement_grace_seconds` | yes | type="integer"; minimum=0 |  |
| <a id="s-cc0fa14a7d"></a>`retirement_policy` | yes | type="string"; enum=["retain","retire-after-verified-output"] |  |

##### <a id="s-c3d365e968"></a>definition `ProcessingClaimPlanDocument`

- <a id="s-a24c759ac1"></a>`type`: `"object"`
- <a id="s-d844854b6d"></a>`additionalProperties`: `false`
- `if`: [See definition `ProcessingClaimPlanDocument` · `if`](#s-10bc900612)
- <a id="s-2fd1ad586e"></a>`required`: `["execution_id","controller_evidence","controller_evidence_sha256","operation","inputs","artifacts","retirement_policy","retirement_grace_seconds","sealed_at"]`
- `then`: [See definition `ProcessingClaimPlanDocument` · `then`](#s-00a3278eb6)

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-afef702ed4"></a>`artifacts` | yes | [ArtifactSetAuthorityDocument](#s-7c0e8d7455) |  |
| <a id="s-03fff9d31b"></a>`controller_evidence` | yes | type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=16777216; x-riverhog-extent={"policy":"contract_max","reason":"bounded-controller-evidence-envelope"} |  |
| <a id="s-887f7e5447"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-294035e4ba"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-058a20576f"></a>`inputs` | yes | [ExactSetAuthorityDocument](#s-52c5516811) |  |
| <a id="s-097037e371"></a>`operation` | yes | [OperationIdentityDocument](#s-e96e2dd600) |  |
| <a id="s-4bd5171262"></a>`retirement_grace_seconds` | yes | type="integer"; minimum=0 |  |
| <a id="s-d1762d6c5c"></a>`retirement_policy` | yes | type="string"; enum=["retain","retire-after-verified-output"] |  |
| <a id="s-86aec53513"></a>`sealed_at` | yes | type="string"; maxLength=64; minLength=1 |  |

##### <a id="s-9dd4ce27cb"></a>definition `ReceivingSetDocument`

- <a id="s-9e92eadf6c"></a>`type`: `"object"`
- <a id="s-c4d6a0a0f6"></a>`additionalProperties`: `false`
- <a id="s-21790251fb"></a>`required`: `["state","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fee71b8ab3"></a>`authority` | no | anyOf=[([ExactSetAuthorityDocument](#s-52c5516811)); (type="null")]; default=null |  |
| <a id="s-6d640da22c"></a>`count` | yes | type="integer"; minimum=0 |  |
| <a id="s-5d1c7d8438"></a>`state` | yes | type="string"; enum=["receiving","sealed"] |  |

##### <a id="s-c359acdbc4"></a>definition `ProcessingClaimOutcomeSettlementDocument` · `if`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e171488835"></a>`retirement_policy` | no | const="retain" |  |

##### <a id="s-e65195034c"></a>definition `ProcessingClaimOutcomeSettlementDocument` · `then`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ec69ee0b19"></a>`retirement_grace_seconds` | no | const=0 |  |

##### <a id="s-10bc900612"></a>definition `ProcessingClaimPlanDocument` · `if`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f234943b7c"></a>`retirement_policy` | no | const="retain" |  |

##### <a id="s-00a3278eb6"></a>definition `ProcessingClaimPlanDocument` · `then`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-37e0041936"></a>`retirement_grace_seconds` | no | const=0 |  |

## Maintained corroboration

### Related interface records

- [validate_claim](riverhog-protocol-processingclaimdocument-validate-claim.md)
- [get](riverhog-protocol-processingclaimdocument-get.md)
- [__getitem__](riverhog-protocol-processingclaimdocument-getitem.md)

## Governing policies

- <a id="pa-f88ab499ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce933f60e39e3c6d2114bd5835a9ac2e04c77240bfae2d434503349bfedbb70c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
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
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "ExactSetAuthorityDocument": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "count",
            "sha256"
          ],
          "type": "object"
        },
        "OperationIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "id": {
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
            "sha256"
          ],
          "type": "object"
        },
        "OutcomeSetDocument": {
          "additionalProperties": false,
          "properties": {
            "authority": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ExactSetAuthorityDocument"
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
            "failure": {
              "anyOf": [
                {
                  "maxLength": 1000,
                  "minLength": 1,
                  "type": "string"
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
                "sealing",
                "sealed",
                "failed"
              ],
              "type": "string"
            }
          },
          "required": [
            "state",
            "count"
          ],
          "type": "object"
        },
        "ProcessingClaimConsumerDocument": {
          "additionalProperties": false,
          "properties": {
            "app": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "key_id": {
              "anyOf": [
                {
                  "maxLength": 300,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "app"
          ],
          "type": "object"
        },
        "ProcessingClaimOutcomeSettlementDocument": {
          "additionalProperties": false,
          "if": {
            "properties": {
              "retirement_policy": {
                "const": "retain"
              }
            }
          },
          "properties": {
            "outcomes": {
              "$ref": "#/$defs/ExactSetAuthorityDocument"
            },
            "retirement_grace_seconds": {
              "minimum": 0,
              "type": "integer"
            },
            "retirement_policy": {
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "type": "string"
            }
          },
          "required": [
            "outcomes",
            "retirement_policy",
            "retirement_grace_seconds"
          ],
          "then": {
            "properties": {
              "retirement_grace_seconds": {
                "const": 0
              }
            }
          },
          "type": "object"
        },
        "ProcessingClaimPlanDocument": {
          "additionalProperties": false,
          "if": {
            "properties": {
              "retirement_policy": {
                "const": "retain"
              }
            }
          },
          "properties": {
            "artifacts": {
              "$ref": "#/$defs/ArtifactSetAuthorityDocument"
            },
            "controller_evidence": {
              "additionalProperties": true,
              "type": "object",
              "x-riverhog-encoded-bytes-max": 16777216,
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-controller-evidence-envelope"
              }
            },
            "controller_evidence_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "execution_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "inputs": {
              "$ref": "#/$defs/ExactSetAuthorityDocument"
            },
            "operation": {
              "$ref": "#/$defs/OperationIdentityDocument"
            },
            "retirement_grace_seconds": {
              "minimum": 0,
              "type": "integer"
            },
            "retirement_policy": {
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "type": "string"
            },
            "sealed_at": {
              "maxLength": 64,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "execution_id",
            "controller_evidence",
            "controller_evidence_sha256",
            "operation",
            "inputs",
            "artifacts",
            "retirement_policy",
            "retirement_grace_seconds",
            "sealed_at"
          ],
          "then": {
            "properties": {
              "retirement_grace_seconds": {
                "const": 0
              }
            }
          },
          "type": "object"
        },
        "ReceivingSetDocument": {
          "additionalProperties": false,
          "properties": {
            "authority": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ExactSetAuthorityDocument"
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
            }
          },
          "required": [
            "state",
            "count"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "allOf": [
        {
          "else": {
            "properties": {
              "settled_at": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "enum": [
                  "settled",
                  "retiring",
                  "released"
                ]
              }
            }
          },
          "then": {
            "properties": {
              "settled_at": {
                "type": "string"
              }
            },
            "required": [
              "settled_at"
            ]
          }
        },
        {
          "else": {
            "properties": {
              "abandoned_at": {
                "type": "null"
              },
              "abandonment_reason": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "const": "abandoned"
              }
            }
          },
          "then": {
            "properties": {
              "abandoned_at": {
                "type": "string"
              },
              "abandonment_reason": {
                "type": "string"
              }
            },
            "required": [
              "abandoned_at",
              "abandonment_reason"
            ]
          }
        },
        {
          "else": {
            "properties": {
              "released_at": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "const": "released"
              }
            }
          },
          "then": {
            "properties": {
              "released_at": {
                "type": "string"
              }
            },
            "required": [
              "released_at"
            ]
          }
        }
      ],
      "properties": {
        "abandoned_at": {
          "anyOf": [
            {
              "maxLength": 64,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "abandonment_reason": {
          "anyOf": [
            {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "consumer": {
          "$ref": "#/$defs/ProcessingClaimConsumerDocument"
        },
        "created_at": {
          "maxLength": 64,
          "minLength": 1,
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
          "const": "riverhog-processing-claim/v1",
          "type": "string"
        },
        "id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "inputs": {
          "$ref": "#/$defs/ReceivingSetDocument"
        },
        "outcome_settlement": {
          "anyOf": [
            {
              "$ref": "#/$defs/ProcessingClaimOutcomeSettlementDocument"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "outcomes": {
          "$ref": "#/$defs/OutcomeSetDocument"
        },
        "output_collection_id": {
          "anyOf": [
            {
              "$ref": "#/$defs/CollectionId"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "plan": {
          "anyOf": [
            {
              "$ref": "#/$defs/ProcessingClaimPlanDocument"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "purpose": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "released_at": {
          "anyOf": [
            {
              "maxLength": 64,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "settled_at": {
          "anyOf": [
            {
              "maxLength": 64,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "state": {
          "enum": [
            "active",
            "settled",
            "retiring",
            "abandoned",
            "released"
          ],
          "type": "string"
        },
        "updated_at": {
          "maxLength": 64,
          "minLength": 1,
          "type": "string"
        },
        "work_document": {
          "additionalProperties": true,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 4194304,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-work-document-envelope"
          }
        },
        "work_document_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "format",
        "id",
        "work_id",
        "consumer",
        "purpose",
        "state",
        "fence",
        "expires_at",
        "created_at",
        "updated_at",
        "work_document",
        "work_document_sha256",
        "inputs",
        "outcomes"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-processing-claim/v1'], id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], work_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], consumer: riverhog_protocol.collection_workflow_transport.ProcessingClaimConsumerDocument, purpose: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['active', 'settled', 'retiring', 'abandoned', 'released'], fence: Annotated[int, Ge(ge=1)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], created_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], settled_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandoned_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, abandonment_reason: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, released_at: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[MinLen(min_length=1), MaxLen(max_length=64)])]] = None, output_collection_id: CollectionId | None = None, work_document: dict[str, typing.Any], work_document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: riverhog_protocol.collection_workflow_transport.ReceivingSetDocument, plan: riverhog_protocol.collection_workflow_transport.ProcessingClaimPlanDocument | None = None, outcomes: riverhog_protocol.collection_workflow_transport.OutcomeSetDocument, outcome_settlement: riverhog_protocol.collection_workflow_transport.ProcessingClaimOutcomeSettlementDocument | None = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimDocument",
  "unit": "export"
}
```

</details>
