# riverhog_protocol.ProcessingClaimSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimsettledocument:fa97ca06cb -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-01be60b278"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd4b96785f"></a>`derivation` | yes | #/$defs/CollectionDerivationDocument |  |
| <a id="s-011a82f456"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-84a0f0c6d4"></a>`outcome` | no | anyOf=#/$defs/ProcessingOutcomeBindingDocument \| type="null" |  |
| <a id="s-67ff0b4dc2"></a>`output_collection_id` | yes | #/$defs/CollectionId |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-bcbe294d68"></a>`ArtifactDispositionSetIdentityDocument` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-adf64e03c7"></a>`ClaimFenceDocument` | type="object"; fields=`fence`, `id`; additional keys=`additionalProperties`, `required` |
| <a id="s-2bd8d62893"></a>`CollectionDerivationDocument` | type="object"; fields=`artifact_set_sha256`, `claim`, `controller_evidence`, `controller_evidence_sha256`, `disposition_set`, `execution_envelope_sha256`, `execution_id`, `execution_sha256`, `format`, `input_set_sha256`, `operation`, `recipe`; additional keys=`additionalProperties`, `required` |
| <a id="s-e37ce6215f"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-769008a64e"></a>`OperationIdentityDocument` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-c7e96fccf8"></a>`ProcessingOutcomeBindingDocument` | type="object"; fields=`claim_id`, `fence`, `outcome_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-7b57800bfc"></a>`RecipeIdentityDocument` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-b29df7919f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimSettleDocument`

### Exact owned JSON

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
