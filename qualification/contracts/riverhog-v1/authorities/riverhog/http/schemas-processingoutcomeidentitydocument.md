# schemas: ProcessingOutcomeIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingoutcomeidentitydocument:8d55fe5a2a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-b3d0325963"></a>
- <a id="s-8002a33e98"></a>`title`: ProcessingOutcomeIdentityDocument
- <a id="s-e47eff50e3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d5ecccb026"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-407976f811"></a>`outcome_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-839f8b81bf"></a>`output_collection` | yes | #/components/schemas/CollectionRootIdentityDocument |  |
| <a id="s-593ebdabb5"></a>`source_claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field derivation_sha256](#s-d5ecccb026) | `length · characters · fixed` | shared above |
| [field source_claim_id](#s-593ebdabb5) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)

## Governing policies

- <a id="pa-852cc2dc36"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-b9214fb0dc"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingOutcomeIdentityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e3282db7bb36155568d93234ae4016c5e043e2c6ac6998425bb60fe3a338509 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "derivation_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Derivation Sha256",
      "type": "string"
    },
    "outcome_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Outcome Id",
      "type": "string"
    },
    "output_collection": {
      "$ref": "#/components/schemas/CollectionRootIdentityDocument"
    },
    "source_claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Claim Id",
      "type": "string"
    }
  },
  "required": [
    "outcome_id",
    "source_claim_id",
    "output_collection",
    "derivation_sha256"
  ],
  "title": "ProcessingOutcomeIdentityDocument",
  "type": "object"
}
```
