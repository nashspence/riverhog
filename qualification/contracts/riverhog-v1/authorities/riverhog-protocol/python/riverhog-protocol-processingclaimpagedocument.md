# riverhog_protocol.ProcessingClaimPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimpagedocument:0157b994c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-885d7fc9be"></a>
- <a id="s-2e64bcc8f5"></a>`distribution`: `riverhog-protocol`
- <a id="s-8ab1096825"></a>`module`: `riverhog_protocol`
- <a id="s-c6423f7606"></a>`name`: `ProcessingClaimPageDocument`
- <a id="s-22e70d1800"></a>`unit`: `export`

### Declared structure

- <a id="s-677df3beb5"></a>`kind`: `"class"`
- <a id="s-4959e67e4d"></a>`signature`: `"'(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken \| None, sort: ProcessingClaimSort, order: SortOrder, filters: riverhog_protocol.collection_workflow_transport.ProcessingClaimFiltersDocument, claims: list[riverhog_protocol.collection_workflow_transport.ProcessingClaimDocument]) -> None'"`

#### Validated model schema

<a id="s-25572c69a7"></a>

- <a id="s-6fd821e23a"></a>`type`: `"object"`
- <a id="s-1d8e8a7236"></a>`additionalProperties`: `false`
- <a id="s-4be50bac2e"></a>`required`: `["page_size","next_page_token","sort","order","filters","claims"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9e01642313"></a>`claims` | yes | type="array"; items=([ProcessingClaimDocument](#s-0729737436)) |  |
| <a id="s-431719c221"></a>`filters` | yes | [ProcessingClaimFiltersDocument](#s-65f1deb5e5) |  |
| <a id="s-3cde4d68f9"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](#s-45f0983f59)); (type="null")] |  |
| <a id="s-496eb27f70"></a>`order` | yes | [SortOrder](#s-8b46b0fee4) |  |
| <a id="s-6a13b81f20"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-8b86035dac"></a>`sort` | yes | [ProcessingClaimSort](#s-3db543803a) |  |

##### Definitions

- [ArtifactSetAuthorityDocument](#s-e2e3e3e591)
- [BrowsePageToken](#s-45f0983f59)
- [CollectionId](#s-40fd4aa997)
- [ExactSetAuthorityDocument](#s-b87ccb9da7)
- [OperationIdentityDocument](#s-e05dec9278)
- [OutcomeSetDocument](#s-5b13a97ed1)
- [ProcessingClaimConsumerDocument](#s-4bf8bbae51)
- [ProcessingClaimDocument](#s-0729737436)
- [ProcessingClaimFiltersDocument](#s-65f1deb5e5)
- [ProcessingClaimOutcomeSettlementDocument](#s-e159af631a)
- [ProcessingClaimPlanDocument](#s-3bbf0a4010)
- [ProcessingClaimSort](#s-3db543803a)
- [ReceivingSetDocument](#s-4631ed20b1)
- [SortOrder](#s-8b46b0fee4)

##### <a id="s-e2e3e3e591"></a>definition `ArtifactSetAuthorityDocument`

- <a id="s-b1d19cdb82"></a>`type`: `"object"`
- <a id="s-dd8d4044c7"></a>`additionalProperties`: `false`
- <a id="s-7ed065e175"></a>`required`: `["count","sha256","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-16242acb45"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-1379dcee7e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-68a4257eeb"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-45f0983f59"></a>definition `BrowsePageToken`

- <a id="s-95fc253c97"></a>`type`: `"string"`
- <a id="s-0a6c756e59"></a>`maxLength`: `8192`
- <a id="s-045f60330c"></a>`minLength`: `1`

##### <a id="s-40fd4aa997"></a>definition `CollectionId`

- <a id="s-102416988b"></a>`type`: `"integer"`
- <a id="s-1879a55693"></a>`minimum`: `1`

##### <a id="s-b87ccb9da7"></a>definition `ExactSetAuthorityDocument`

- <a id="s-715672d1a1"></a>`type`: `"object"`
- <a id="s-0a2f41b66d"></a>`additionalProperties`: `false`
- <a id="s-c4ca9fa08a"></a>`required`: `["count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-74779b7eb3"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-0fb46c9599"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e05dec9278"></a>definition `OperationIdentityDocument`

- <a id="s-a6c28577fe"></a>`type`: `"object"`
- <a id="s-3ba2ab02b6"></a>`additionalProperties`: `false`
- <a id="s-a92f55bba6"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25588d5a99"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-42b680181d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-5b13a97ed1"></a>definition `OutcomeSetDocument`

- <a id="s-4d78062ec6"></a>`type`: `"object"`
- <a id="s-ca95ce9c8e"></a>`additionalProperties`: `false`
- <a id="s-afec08a7f5"></a>`required`: `["state","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1fda72619f"></a>`authority` | no | anyOf=[([ExactSetAuthorityDocument](#s-b87ccb9da7)); (type="null")]; default=null |  |
| <a id="s-1386701376"></a>`count` | yes | type="integer"; minimum=0 |  |
| <a id="s-37fca3d541"></a>`failure` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; default=null |  |
| <a id="s-df1f0694f5"></a>`state` | yes | type="string"; enum=["receiving","sealing","sealed","failed"] |  |

##### <a id="s-4bf8bbae51"></a>definition `ProcessingClaimConsumerDocument`

- <a id="s-b94c3c344a"></a>`type`: `"object"`
- <a id="s-9182412414"></a>`additionalProperties`: `false`
- <a id="s-feb6502fe0"></a>`required`: `["app"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c3b9ebb342"></a>`app` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-381d1fb748"></a>`key_id` | no | anyOf=[(type="string"; maxLength=300; minLength=1); (type="null")]; default=null |  |

##### <a id="s-0729737436"></a>definition `ProcessingClaimDocument`

- <a id="s-76476041ba"></a>`type`: `"object"`
- <a id="s-9995d4a779"></a>`additionalProperties`: `false`
- <a id="s-18daf067bc"></a>`required`: `["format","id","work_id","consumer","purpose","state","fence","expires_at","created_at","updated_at","work_document","work_document_sha256","inputs","outcomes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-24c2850fae"></a>`abandoned_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-8f75c8d14a"></a>`abandonment_reason` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; default=null |  |
| <a id="s-2eb7221fd6"></a>`consumer` | yes | [ProcessingClaimConsumerDocument](#s-4bf8bbae51) |  |
| <a id="s-11983e0a3f"></a>`created_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-dc73b48a31"></a>`expires_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-9e52be538b"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-ab4ec9779c"></a>`format` | yes | type="string"; const="riverhog-processing-claim/v1" |  |
| <a id="s-1d896f1cd1"></a>`id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-00eb886074"></a>`inputs` | yes | [ReceivingSetDocument](#s-4631ed20b1) |  |
| <a id="s-37d85f0551"></a>`outcome_settlement` | no | anyOf=[([ProcessingClaimOutcomeSettlementDocument](#s-e159af631a)); (type="null")]; default=null |  |
| <a id="s-01c79f3539"></a>`outcomes` | yes | [OutcomeSetDocument](#s-5b13a97ed1) |  |
| <a id="s-9c887ff265"></a>`output_collection_id` | no | anyOf=[([CollectionId](#s-40fd4aa997)); (type="null")]; default=null |  |
| <a id="s-22e6d3db1d"></a>`plan` | no | anyOf=[([ProcessingClaimPlanDocument](#s-3bbf0a4010)); (type="null")]; default=null |  |
| <a id="s-5d99764df7"></a>`purpose` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-15149cdf4e"></a>`released_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-3bfd7138b4"></a>`settled_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-b41339bc2b"></a>`state` | yes | type="string"; enum=["active","settled","retiring","abandoned","released"] |  |
| <a id="s-8a3c3355be"></a>`updated_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-9e31fb61f1"></a>`work_document` | yes | type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4194304; x-riverhog-extent={"policy":"contract_max","reason":"bounded-work-document-envelope"} |  |
| <a id="s-e5089e0a76"></a>`work_document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-35d5b3c6fd"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

###### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-35a36bd01d"></a>1 | properties={state: (enum=["settled","retiring","released"])} | properties={settled_at: (type="string")}; required=["settled_at"] | properties={settled_at: (type="null")} |
| <a id="s-be5fdbeb73"></a>2 | properties={state: (const="abandoned")} | properties={abandoned_at: (type="string"); abandonment_reason: (type="string")}; required=["abandoned_at","abandonment_reason"] | properties={abandoned_at: (type="null"); abandonment_reason: (type="null")} |
| <a id="s-6cc34a2b37"></a>3 | properties={state: (const="released")} | properties={released_at: (type="string")}; required=["released_at"] | properties={released_at: (type="null")} |

##### <a id="s-65f1deb5e5"></a>definition `ProcessingClaimFiltersDocument`

- <a id="s-106ea94e6a"></a>`type`: `"object"`
- <a id="s-3676dbfe44"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e4a7c8710f"></a>`state` | no | anyOf=[(type="string"; enum=["active","settled","retiring","abandoned","released"]); (type="null")]; default=null |  |

##### <a id="s-e159af631a"></a>definition `ProcessingClaimOutcomeSettlementDocument`

- <a id="s-283f62d030"></a>`type`: `"object"`
- <a id="s-0c1eb21d64"></a>`additionalProperties`: `false`
- `if`: [See definition `ProcessingClaimOutcomeSettlementDocument` · `if`](#s-ee9c5b5300)
- <a id="s-53451c86b2"></a>`required`: `["outcomes","retirement_policy","retirement_grace_seconds"]`
- `then`: [See definition `ProcessingClaimOutcomeSettlementDocument` · `then`](#s-9414ea9539)

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dec0678355"></a>`outcomes` | yes | [ExactSetAuthorityDocument](#s-b87ccb9da7) |  |
| <a id="s-5fd1474596"></a>`retirement_grace_seconds` | yes | type="integer"; minimum=0 |  |
| <a id="s-d4fe79f988"></a>`retirement_policy` | yes | type="string"; enum=["retain","retire-after-verified-output"] |  |

##### <a id="s-3bbf0a4010"></a>definition `ProcessingClaimPlanDocument`

- <a id="s-d7ac245396"></a>`type`: `"object"`
- <a id="s-8e9ade097b"></a>`additionalProperties`: `false`
- `if`: [See definition `ProcessingClaimPlanDocument` · `if`](#s-57c5c26786)
- <a id="s-205b08c15a"></a>`required`: `["execution_id","controller_evidence","controller_evidence_sha256","operation","inputs","artifacts","retirement_policy","retirement_grace_seconds","sealed_at"]`
- `then`: [See definition `ProcessingClaimPlanDocument` · `then`](#s-9d16068bcf)

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-632cfa2299"></a>`artifacts` | yes | [ArtifactSetAuthorityDocument](#s-e2e3e3e591) |  |
| <a id="s-fa4051fad3"></a>`controller_evidence` | yes | type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=16777216; x-riverhog-extent={"policy":"contract_max","reason":"bounded-controller-evidence-envelope"} |  |
| <a id="s-aff41b1c6d"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a62dcfe550"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8a3e1bf0b7"></a>`inputs` | yes | [ExactSetAuthorityDocument](#s-b87ccb9da7) |  |
| <a id="s-c54d1d1aa2"></a>`operation` | yes | [OperationIdentityDocument](#s-e05dec9278) |  |
| <a id="s-b7d410c8ce"></a>`retirement_grace_seconds` | yes | type="integer"; minimum=0 |  |
| <a id="s-dc68417f6b"></a>`retirement_policy` | yes | type="string"; enum=["retain","retire-after-verified-output"] |  |
| <a id="s-42cee0c720"></a>`sealed_at` | yes | type="string"; maxLength=64; minLength=1 |  |

##### <a id="s-3db543803a"></a>definition `ProcessingClaimSort`

- <a id="s-c1f788223a"></a>`type`: `"string"`
- <a id="s-47f3fdfda3"></a>`enum`: `["created_at","updated_at","expires_at","state","work_id","execution_id"]`

##### <a id="s-4631ed20b1"></a>definition `ReceivingSetDocument`

- <a id="s-4aa839abc0"></a>`type`: `"object"`
- <a id="s-ac52358434"></a>`additionalProperties`: `false`
- <a id="s-e2ec144fa6"></a>`required`: `["state","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-76cc034fdd"></a>`authority` | no | anyOf=[([ExactSetAuthorityDocument](#s-b87ccb9da7)); (type="null")]; default=null |  |
| <a id="s-7c1b19f001"></a>`count` | yes | type="integer"; minimum=0 |  |
| <a id="s-2e3cdf0cb5"></a>`state` | yes | type="string"; enum=["receiving","sealed"] |  |

##### <a id="s-8b46b0fee4"></a>definition `SortOrder`

- <a id="s-110d2ac44c"></a>`type`: `"string"`
- <a id="s-a882210e12"></a>`enum`: `["asc","desc"]`

##### <a id="s-ee9c5b5300"></a>definition `ProcessingClaimOutcomeSettlementDocument` · `if`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-04c12d062f"></a>`retirement_policy` | no | const="retain" |  |

##### <a id="s-9414ea9539"></a>definition `ProcessingClaimOutcomeSettlementDocument` · `then`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-556e5cc943"></a>`retirement_grace_seconds` | no | const=0 |  |

##### <a id="s-57c5c26786"></a>definition `ProcessingClaimPlanDocument` · `if`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fcb0723e11"></a>`retirement_policy` | no | const="retain" |  |

##### <a id="s-9d16068bcf"></a>definition `ProcessingClaimPlanDocument` · `then`


###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f16a2b9bd4"></a>`retirement_grace_seconds` | no | const=0 |  |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingclaimpagedocument-getitem.md)
- [get](riverhog-protocol-processingclaimpagedocument-get.md)

## Governing policies

- <a id="pa-efe7905141"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimPageDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3b7ef2cf9406984581c0a3a9efbd6954e5b5ac99d5d5a0ae8f3e245aec712ce -->

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
        "BrowsePageToken": {
          "maxLength": 8192,
          "minLength": 1,
          "type": "string"
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
        "ProcessingClaimDocument": {
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
        "ProcessingClaimFiltersDocument": {
          "additionalProperties": false,
          "properties": {
            "state": {
              "anyOf": [
                {
                  "enum": [
                    "active",
                    "settled",
                    "retiring",
                    "abandoned",
                    "released"
                  ],
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
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
        "ProcessingClaimSort": {
          "enum": [
            "created_at",
            "updated_at",
            "expires_at",
            "state",
            "work_id",
            "execution_id"
          ],
          "type": "string"
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
        },
        "SortOrder": {
          "enum": [
            "asc",
            "desc"
          ],
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "claims": {
          "items": {
            "$ref": "#/$defs/ProcessingClaimDocument"
          },
          "type": "array"
        },
        "filters": {
          "$ref": "#/$defs/ProcessingClaimFiltersDocument"
        },
        "next_page_token": {
          "anyOf": [
            {
              "$ref": "#/$defs/BrowsePageToken"
            },
            {
              "type": "null"
            }
          ]
        },
        "order": {
          "$ref": "#/$defs/SortOrder"
        },
        "page_size": {
          "maximum": 100,
          "minimum": 1,
          "type": "integer"
        },
        "sort": {
          "$ref": "#/$defs/ProcessingClaimSort"
        }
      },
      "required": [
        "page_size",
        "next_page_token",
        "sort",
        "order",
        "filters",
        "claims"
      ],
      "type": "object"
    },
    "signature": "'(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, sort: ProcessingClaimSort, order: SortOrder, filters: riverhog_protocol.collection_workflow_transport.ProcessingClaimFiltersDocument, claims: list[riverhog_protocol.collection_workflow_transport.ProcessingClaimDocument]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimPageDocument",
  "unit": "export"
}
```

</details>
