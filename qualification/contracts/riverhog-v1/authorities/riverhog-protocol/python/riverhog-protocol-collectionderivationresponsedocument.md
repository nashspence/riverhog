# riverhog_protocol.CollectionDerivationResponseDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivationres-d949202780:89f510ae30 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c3d08fda3a"></a>
- <a id="s-ac435e56c4"></a>`distribution`: `riverhog-protocol`
- <a id="s-cca0879585"></a>`module`: `riverhog_protocol`
- <a id="s-9d3487bfae"></a>`name`: `CollectionDerivationResponseDocument`
- <a id="s-f80e25846f"></a>`unit`: `export`

### Declared structure

- <a id="s-b98b7a96c7"></a>`kind`: `"class"`
- <a id="s-5e7b8d2be1"></a>`signature`: `"\"(*, collection_id: CollectionId, document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], derivation: riverhog_protocol.collection_workflow_transport.CollectionDerivationDocument) -> None\""`

#### Validated model schema

<a id="s-71da214ae0"></a>
- <a id="s-adbab16ff0"></a>`title`: CollectionDerivationResponseDocument
- <a id="s-3a49fd707c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5dc08d71ba"></a>`collection_id` | yes | #/$defs/CollectionId |  |
| <a id="s-ff2e6d297a"></a>`derivation` | yes | #/$defs/CollectionDerivationDocument |  |
| <a id="s-fe8fb1e8ae"></a>`document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-e39f407cdc"></a>`ArtifactDispositionSetIdentityDocument` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-1bcca920fa"></a>`ClaimFenceDocument` | type="object"; fields=`fence`, `id`; additional keys=`additionalProperties`, `required` |
| <a id="s-c39938b744"></a>`CollectionDerivationDocument` | type="object"; fields=`artifact_set_sha256`, `claim`, `controller_evidence`, `controller_evidence_sha256`, `disposition_set`, `execution_envelope_sha256`, `execution_id`, `execution_sha256`, `format`, `input_set_sha256`, `operation`, `recipe`; additional keys=`additionalProperties`, `required` |
| <a id="s-71747045d5"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-28d97b8c2e"></a>`OperationIdentityDocument` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-dd912b8d9e"></a>`RecipeIdentityDocument` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionDerivationResponseDocument.validate_identity](riverhog-protocol-collectionderivationresponsedocument-validate-identity.md)

## Governing policies

- <a id="pa-413f4ca3f0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivationResponseDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ac02b33e293839e09376147743cc27ea84f51f84dc7790d91f2f907e4c6fffe -->

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
              "title": "Disposition Count",
              "type": "integer"
            },
            "output_artifact_count": {
              "minimum": 1,
              "title": "Output Artifact Count",
              "type": "integer"
            },
            "output_edge_count": {
              "minimum": 1,
              "title": "Output Edge Count",
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "disposition_count",
            "output_edge_count",
            "output_artifact_count",
            "sha256"
          ],
          "title": "ArtifactDispositionSetIdentityDocument",
          "type": "object"
        },
        "ClaimFenceDocument": {
          "additionalProperties": false,
          "properties": {
            "fence": {
              "minimum": 1,
              "title": "Fence",
              "type": "integer"
            },
            "id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Id",
              "type": "string"
            }
          },
          "required": [
            "id",
            "fence"
          ],
          "title": "ClaimFenceDocument",
          "type": "object"
        },
        "CollectionDerivationDocument": {
          "additionalProperties": false,
          "properties": {
            "artifact_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Artifact Set Sha256",
              "type": "string"
            },
            "claim": {
              "$ref": "#/$defs/ClaimFenceDocument"
            },
            "controller_evidence": {
              "additionalProperties": true,
              "title": "Controller Evidence",
              "type": "object",
              "x-riverhog-encoded-bytes-max": 16777216,
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-controller-evidence-envelope"
              }
            },
            "controller_evidence_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Controller Evidence Sha256",
              "type": "string"
            },
            "disposition_set": {
              "$ref": "#/$defs/ArtifactDispositionSetIdentityDocument"
            },
            "execution_envelope_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Execution Envelope Sha256",
              "type": "string"
            },
            "execution_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Execution Id",
              "type": "string"
            },
            "execution_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Execution Sha256",
              "type": "string"
            },
            "format": {
              "const": "riverhog-collection-derivation/v1",
              "title": "Format",
              "type": "string"
            },
            "input_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Input Set Sha256",
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
          "title": "CollectionDerivationDocument",
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
              "title": "Id",
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
            "sha256"
          ],
          "title": "OperationIdentityDocument",
          "type": "object"
        },
        "RecipeIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Id",
              "type": "string"
            },
            "revision": {
              "minimum": 1,
              "title": "Revision",
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "sha256"
          ],
          "title": "RecipeIdentityDocument",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "derivation": {
          "$ref": "#/$defs/CollectionDerivationDocument"
        },
        "document_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Document Sha256",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "document_sha256",
        "derivation"
      ],
      "title": "CollectionDerivationResponseDocument",
      "type": "object"
    },
    "signature": "\"(*, collection_id: CollectionId, document_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], derivation: riverhog_protocol.collection_workflow_transport.CollectionDerivationDocument) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionDerivationResponseDocument",
  "unit": "export"
}
```
