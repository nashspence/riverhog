# schemas: CollectionDerivationDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionderivationdocument:78d3d731d2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 8 |

## External contract

- `title`: CollectionDerivationDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `claim` | yes | #/components/schemas/ClaimFenceDocument |  |
| `controller_evidence` | yes | type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| `controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `disposition_set` | yes | #/components/schemas/ArtifactDispositionSetIdentityDocument |  |
| `execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `format` | yes | type="string"; const="riverhog-collection-derivation/v1" |  |
| `input_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `operation` | yes | #/components/schemas/OperationIdentityDocument |  |
| `recipe` | yes | #/components/schemas/RecipeIdentityDocument |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| encoded-size | bytes | `contract_max` | maximum=16777216, reason=bounded-controller-evidence-envelope |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)
- [schemas: ClaimFenceDocument](schemas-claimfencedocument.md)
- [schemas: OperationIdentityDocument](schemas-operationidentitydocument.md)
- [schemas: RecipeIdentityDocument](schemas-recipeidentitydocument.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDerivationDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ce117b7cd1e65da4bf41e7e3c1caf9588903351cad722ee7fbc1f7ffd042a2c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifact_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Artifact Set Sha256",
      "type": "string"
    },
    "claim": {
      "$ref": "#/components/schemas/ClaimFenceDocument"
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
      "$ref": "#/components/schemas/ArtifactDispositionSetIdentityDocument"
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
      "$ref": "#/components/schemas/OperationIdentityDocument"
    },
    "recipe": {
      "$ref": "#/components/schemas/RecipeIdentityDocument"
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
}
```
