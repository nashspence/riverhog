# schemas: CollectionDerivationDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionderivationdocument:13012e54f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ca21501997"></a>

- <a id="s-7a518e74f8"></a>`type`: `"object"`
- <a id="s-c551348219"></a>`additionalProperties`: `false`
- <a id="s-27363f08ff"></a>`required`: `["format","execution_id","claim","recipe","operation","input_set_sha256","artifact_set_sha256","execution_envelope_sha256","execution_sha256","controller_evidence","controller_evidence_sha256","disposition_set"]`
- <a id="s-388b5adee8"></a>`title`: `"CollectionDerivationDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5950ee9b49"></a>`artifact_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Set Sha256" |  |
| <a id="s-ba90eb4af8"></a>`claim` | yes | [ClaimFenceDocument](schemas-claimfencedocument.md) |  |
| <a id="s-b4e114877f"></a>`controller_evidence` | yes | type="object"; additionalProperties=(any JSON value); title="Controller Evidence"; x-riverhog-encoded-bytes-max=16777216; x-riverhog-extent={"policy":"contract_max","reason":"bounded-controller-evidence-envelope"} |  |
| <a id="s-86b6e19970"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Controller Evidence Sha256" |  |
| <a id="s-bd5a5d40f4"></a>`disposition_set` | yes | [ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md) |  |
| <a id="s-ad4497c421"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Envelope Sha256" |  |
| <a id="s-ace8b4689c"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Id" |  |
| <a id="s-9babeae319"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-a259beccb3"></a>`format` | yes | type="string"; const="riverhog-collection-derivation/v1"; title="Format" |  |
| <a id="s-666e892b51"></a>`input_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Input Set Sha256" |  |
| <a id="s-a2f6926964"></a>`operation` | yes | [OperationIdentityDocument](schemas-operationidentitydocument.md) |  |
| <a id="s-ada19b2ae3"></a>`recipe` | yes | [RecipeIdentityDocument](schemas-recipeidentitydocument.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence](#s-b4e114877f) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_set_sha256](#s-5950ee9b49) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field controller_evidence](#s-b4e114877f) | `encoded-size · bytes · contract_max` | maximum=16777216; reason="bounded-controller-evidence-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field controller_evidence_sha256](#s-86b6e19970) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field execution_envelope_sha256](#s-ad4497c421) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field execution_id](#s-ace8b4689c) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field execution_sha256](#s-9babeae319) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field input_set_sha256](#s-666e892b51) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)
- [ClaimFenceDocument](schemas-claimfencedocument.md)
- [OperationIdentityDocument](schemas-operationidentitydocument.md)
- [RecipeIdentityDocument](schemas-recipeidentitydocument.md)

## Governing policies

- <a id="pa-a2d40b2e96"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-dfe7e3f8b0"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-a6473e1642"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDerivationDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
