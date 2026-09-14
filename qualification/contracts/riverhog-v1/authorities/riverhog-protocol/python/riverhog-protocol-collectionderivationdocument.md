# riverhog_protocol.CollectionDerivationDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivationdocument:b293f3461a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f5b2ba4914"></a>
- <a id="s-06afa26551"></a>`distribution`: `riverhog-protocol`
- <a id="s-c5370bb4e5"></a>`module`: `riverhog_protocol`
- <a id="s-1643a30ba3"></a>`name`: `CollectionDerivationDocument`
- <a id="s-f4ec65c7db"></a>`unit`: `export`

### Declared structure

- <a id="s-29fa654330"></a>`kind`: `"class"`
- <a id="s-2f8fc99c46"></a>`signature`: `"\"(*, format: Literal['riverhog-collection-derivation/v1'], execution_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], claim: riverhog_protocol.collection_workflow_transport.ClaimFenceDocument, recipe: riverhog_protocol.collection_workflow_transport.RecipeIdentityDocument, operation: riverhog_protocol.collection_workflow_transport.OperationIdentityDocument, input_set_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], artifact_set_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_envelope_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], controller_evidence: dict[str, typing.Any], controller_evidence_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], disposition_set: riverhog_protocol.collection_workflow_transport.ArtifactDispositionSetIdentityDocument) -> None\""`

#### Validated model schema

<a id="s-cb6dc849b6"></a>
- <a id="s-3b39327faa"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38a0e9e4dc"></a>`artifact_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fed44d3a76"></a>`claim` | yes | #/$defs/ClaimFenceDocument |  |
| <a id="s-89d798112f"></a>`controller_evidence` | yes | type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-1bbca76389"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3b2702be87"></a>`disposition_set` | yes | #/$defs/ArtifactDispositionSetIdentityDocument |  |
| <a id="s-9161f7331c"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-25b249d782"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fe09d66580"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-396e1982f1"></a>`format` | yes | type="string"; const="riverhog-collection-derivation/v1" |  |
| <a id="s-cd5854722d"></a>`input_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3d42fba637"></a>`operation` | yes | #/$defs/OperationIdentityDocument |  |
| <a id="s-7c5d796147"></a>`recipe` | yes | #/$defs/RecipeIdentityDocument |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-d963af221e"></a>`ArtifactDispositionSetIdentityDocument` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-6a641731d6"></a>`ClaimFenceDocument` | type="object"; fields=`fence`, `id`; additional keys=`additionalProperties`, `required` |
| <a id="s-339962015a"></a>`OperationIdentityDocument` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-d4dc3b83ad"></a>`RecipeIdentityDocument` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionDerivationDocument.validate_derivation](riverhog-protocol-collectionderivationdocument-validate-derivation.md)

## Governing policies

- <a id="pa-356282d768"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivationDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 67c34cffa63818732a0c702f0f622b2b53aa27a56588b90db47864398093c961 -->

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
    "signature": "\"(*, format: Literal['riverhog-collection-derivation/v1'], execution_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], claim: riverhog_protocol.collection_workflow_transport.ClaimFenceDocument, recipe: riverhog_protocol.collection_workflow_transport.RecipeIdentityDocument, operation: riverhog_protocol.collection_workflow_transport.OperationIdentityDocument, input_set_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], artifact_set_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_envelope_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], execution_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], controller_evidence: dict[str, typing.Any], controller_evidence_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], disposition_set: riverhog_protocol.collection_workflow_transport.ArtifactDispositionSetIdentityDocument) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionDerivationDocument",
  "unit": "export"
}
```
