# schemas: CollectionDerivationDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionderivationdocument:78d3d731d2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 8 |

## External contract

<a id="s-ca2150199791"></a>
- <a id="s-388b5adee860"></a>`title`: CollectionDerivationDocument
- <a id="s-7a518e74f817"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5950ee9b4919"></a>`artifact_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ba90eb4af8b3"></a>`claim` | yes | #/components/schemas/ClaimFenceDocument |  |
| <a id="s-b4e114877fc3"></a>`controller_evidence` | yes | type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-86b6e1997033"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bd5a5d40f408"></a>`disposition_set` | yes | #/components/schemas/ArtifactDispositionSetIdentityDocument |  |
| <a id="s-ad4497c42138"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ace8b4689c89"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9babeae31961"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a259beccb319"></a>`format` | yes | type="string"; const="riverhog-collection-derivation/v1" |  |
| <a id="s-666e892b510f"></a>`input_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a2f6926964ab"></a>`operation` | yes | #/components/schemas/OperationIdentityDocument |  |
| <a id="s-ada19b2ae3dd"></a>`recipe` | yes | #/components/schemas/RecipeIdentityDocument |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence](#s-b4e114877fc3) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_set_sha256](#s-5950ee9b4919) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field controller_evidence](#s-b4e114877fc3) | `encoded-size · bytes · contract_max` | maximum=16777216; reason="bounded-controller-evidence-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field controller_evidence_sha256](#s-86b6e1997033) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field execution_envelope_sha256](#s-ad4497c42138) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field execution_id](#s-ace8b4689c89) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field execution_sha256](#s-9babeae31961) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field input_set_sha256](#s-666e892b510f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)
- [schemas: ClaimFenceDocument](schemas-claimfencedocument.md)
- [schemas: OperationIdentityDocument](schemas-operationidentitydocument.md)
- [schemas: RecipeIdentityDocument](schemas-recipeidentitydocument.md)

## Governing policies

- <a id="pa-8112980b8556"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-e82984ce42ee"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-9ad7f28c2a46"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
