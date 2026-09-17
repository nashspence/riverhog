# riverhog_protocol.ProcessingClaimSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimsettledocument:fa97ca06cb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-730bdee45f"></a>
- <a id="s-983c6203e0"></a>`distribution`: `riverhog-protocol`
- <a id="s-620de56e1d"></a>`module`: `riverhog_protocol`
- <a id="s-5c92771092"></a>`name`: `ProcessingClaimSettleDocument`
- <a id="s-bc6159f138"></a>`unit`: `export`

### Declared structure

- <a id="s-8a2f15880b"></a>`kind`: `"class"`
- <a id="s-15b08e67cd"></a>`signature`: `"'(*, fence: Annotated[int, Ge(ge=1)], output_collection_id: CollectionId, derivation: riverhog_protocol.collection_workflow_transport.CollectionDerivationDocument, outcome: riverhog_protocol.collection_workflow_transport.ProcessingOutcomeBindingDocument \| None = None) -> None'"`

#### Validated model schema

<a id="s-3b78000f99"></a>

- <a id="s-01be60b278"></a>`type`: `"object"`
- <a id="s-8ab96069ad"></a>`additionalProperties`: `false`
- <a id="s-5423aafed7"></a>`required`: `["fence","output_collection_id","derivation"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd4b96785f"></a>`derivation` | yes | [CollectionDerivationDocument](#s-2bd8d62893) |  |
| <a id="s-011a82f456"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-84a0f0c6d4"></a>`outcome` | no | anyOf=[([ProcessingOutcomeBindingDocument](#s-c7e96fccf8)); (type="null")]; default=null |  |
| <a id="s-67ff0b4dc2"></a>`output_collection_id` | yes | [CollectionId](#s-e37ce6215f) |  |

##### Definitions

- [ArtifactDispositionSetIdentityDocument](#s-bcbe294d68)
- [ClaimFenceDocument](#s-adf64e03c7)
- [CollectionDerivationDocument](#s-2bd8d62893)
- [CollectionId](#s-e37ce6215f)
- [OperationIdentityDocument](#s-769008a64e)
- [ProcessingOutcomeBindingDocument](#s-c7e96fccf8)
- [RecipeIdentityDocument](#s-7b57800bfc)

##### <a id="s-bcbe294d68"></a>definition `ArtifactDispositionSetIdentityDocument`

- <a id="s-6b761da8f8"></a>`type`: `"object"`
- <a id="s-f99a4692f8"></a>`additionalProperties`: `false`
- <a id="s-e47a5f4018"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6ebf0c7298"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-729ed387ef"></a>`output_artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-c9ad170317"></a>`output_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-330f18cf95"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-adf64e03c7"></a>definition `ClaimFenceDocument`

- <a id="s-8cf805c957"></a>`type`: `"object"`
- <a id="s-dbc0f03a92"></a>`additionalProperties`: `false`
- <a id="s-dbc13f2135"></a>`required`: `["id","fence"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-504601e9c3"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-f57273e012"></a>`id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2bd8d62893"></a>definition `CollectionDerivationDocument`

- <a id="s-ea81121cc8"></a>`type`: `"object"`
- <a id="s-d31d199a16"></a>`additionalProperties`: `false`
- <a id="s-87ee6836b0"></a>`required`: `["format","execution_id","claim","recipe","operation","input_set_sha256","artifact_set_sha256","execution_envelope_sha256","execution_sha256","controller_evidence","controller_evidence_sha256","disposition_set"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-46c59448a7"></a>`artifact_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1f59ed16bc"></a>`claim` | yes | [ClaimFenceDocument](#s-adf64e03c7) |  |
| <a id="s-67d62ddd80"></a>`controller_evidence` | yes | type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=16777216; x-riverhog-extent={"policy":"contract_max","reason":"bounded-controller-evidence-envelope"} |  |
| <a id="s-04e5367438"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-34f6825596"></a>`disposition_set` | yes | [ArtifactDispositionSetIdentityDocument](#s-bcbe294d68) |  |
| <a id="s-54a4d89179"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-aa12042477"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f4d91f287c"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e09d612a64"></a>`format` | yes | type="string"; const="riverhog-collection-derivation/v1" |  |
| <a id="s-7d7236241f"></a>`input_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-527987acc0"></a>`operation` | yes | [OperationIdentityDocument](#s-769008a64e) |  |
| <a id="s-aba4688a18"></a>`recipe` | yes | [RecipeIdentityDocument](#s-7b57800bfc) |  |

##### <a id="s-e37ce6215f"></a>definition `CollectionId`

- <a id="s-7bf946aec7"></a>`type`: `"integer"`
- <a id="s-100ac3d0cb"></a>`minimum`: `1`

##### <a id="s-769008a64e"></a>definition `OperationIdentityDocument`

- <a id="s-06eed4e563"></a>`type`: `"object"`
- <a id="s-830f7b2277"></a>`additionalProperties`: `false`
- <a id="s-a35cfc2308"></a>`required`: `["id","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3d6b19a771"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3e461d4d05"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c7e96fccf8"></a>definition `ProcessingOutcomeBindingDocument`

- <a id="s-20e5693d6a"></a>`type`: `"object"`
- <a id="s-b43dc586db"></a>`additionalProperties`: `false`
- <a id="s-62c9e1e6a1"></a>`required`: `["claim_id","fence","outcome_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aee45cd2fa"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e5b7c1ecfe"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-f13c90ff3a"></a>`outcome_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-7b57800bfc"></a>definition `RecipeIdentityDocument`

- <a id="s-a069a3d0f6"></a>`type`: `"object"`
- <a id="s-2e8ca6b201"></a>`additionalProperties`: `false`
- <a id="s-4a8a078106"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7a850eb03a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-74b5c5fe67"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-ccde48c239"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingclaimsettledocument-getitem.md)
- [get](riverhog-protocol-processingclaimsettledocument-get.md)

## Governing policies

- <a id="pa-b29df7919f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimSettleDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 662882a209eba0ffed6abd726c6e20deff54eedcd23b1beb827023b4adc7f752 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactDispositionSetIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "disposition_count": {
              "minimum": 1,
              "type": "integer"
            },
            "output_artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "output_edge_count": {
              "minimum": 1,
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "disposition_count",
            "output_edge_count",
            "output_artifact_count",
            "sha256"
          ],
          "type": "object"
        },
        "ClaimFenceDocument": {
          "additionalProperties": false,
          "properties": {
            "fence": {
              "minimum": 1,
              "type": "integer"
            },
            "id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "fence"
          ],
          "type": "object"
        },
        "CollectionDerivationDocument": {
          "additionalProperties": false,
          "properties": {
            "artifact_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "claim": {
              "$ref": "#/$defs/ClaimFenceDocument"
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
            "disposition_set": {
              "$ref": "#/$defs/ArtifactDispositionSetIdentityDocument"
            },
            "execution_envelope_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "execution_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "execution_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "riverhog-collection-derivation/v1",
              "type": "string"
            },
            "input_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "operation": {
              "$ref": "#/$defs/OperationIdentityDocument"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeIdentityDocument"
            }
          },
          "required": [
            "format",
            "execution_id",
            "claim",
            "recipe",
            "operation",
            "input_set_sha256",
            "artifact_set_sha256",
            "execution_envelope_sha256",
            "execution_sha256",
            "controller_evidence",
            "controller_evidence_sha256",
            "disposition_set"
          ],
          "type": "object"
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
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
        "ProcessingOutcomeBindingDocument": {
          "additionalProperties": false,
          "properties": {
            "claim_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "fence": {
              "minimum": 1,
              "type": "integer"
            },
            "outcome_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "claim_id",
            "fence",
            "outcome_id"
          ],
          "type": "object"
        },
        "RecipeIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "minimum": 1,
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "derivation": {
          "$ref": "#/$defs/CollectionDerivationDocument"
        },
        "fence": {
          "minimum": 1,
          "type": "integer"
        },
        "outcome": {
          "anyOf": [
            {
              "$ref": "#/$defs/ProcessingOutcomeBindingDocument"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "output_collection_id": {
          "$ref": "#/$defs/CollectionId"
        }
      },
      "required": [
        "fence",
        "output_collection_id",
        "derivation"
      ],
      "type": "object"
    },
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)], output_collection_id: CollectionId, derivation: riverhog_protocol.collection_workflow_transport.CollectionDerivationDocument, outcome: riverhog_protocol.collection_workflow_transport.ProcessingOutcomeBindingDocument | None = None) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimSettleDocument",
  "unit": "export"
}
```

</details>
