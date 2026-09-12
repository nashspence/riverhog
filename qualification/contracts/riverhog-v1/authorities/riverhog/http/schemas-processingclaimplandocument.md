# schemas: ProcessingClaimPlanDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimplandocument:0e187c388f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-834a2c4308"></a>
- <a id="s-f8ed83976a"></a>`title`: ProcessingClaimPlanDocument
- <a id="s-90ca763aba"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a2e1e51a8f"></a>`artifacts` | yes | #/components/schemas/ArtifactSetAuthorityDocument |  |
| <a id="s-c31b744472"></a>`controller_evidence` | yes | type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-827ae4b73c"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-68f89f7550"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6449700174"></a>`inputs` | yes | #/components/schemas/ExactSetAuthorityDocument |  |
| <a id="s-b014f9416d"></a>`operation` | yes | #/components/schemas/OperationIdentityDocument |  |
| <a id="s-6904c65b63"></a>`retirement_grace_seconds` | yes | type="integer"; minimum=0 |  |
| <a id="s-d49cde6da8"></a>`retirement_policy` | yes | type="string"; enum=["retain","retire-after-verified-output"] |  |
| <a id="s-0c58a348ae"></a>`sealed_at` | yes | type="string"; minLength=1; maxLength=64 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence](#s-c31b744472) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence](#s-c31b744472) | `encoded-size · bytes · contract_max` | maximum=16777216; reason="bounded-controller-evidence-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field controller_evidence_sha256](#s-827ae4b73c) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field execution_id](#s-68f89f7550) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field sealed_at](#s-0c58a348ae) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSetAuthorityDocument](schemas-artifactsetauthoritydocument.md)
- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)
- [schemas: OperationIdentityDocument](schemas-operationidentitydocument.md)

## Governing policies

- <a id="pa-5a1121646e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-12560b67b4"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-0c932c017c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimPlanDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c782ec1e7e9f82a2050027187b4f010e5c02d15a1518bc4a009c6856fb7dd042 -->

```json
{
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
      "$ref": "#/components/schemas/ArtifactSetAuthorityDocument"
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
    "execution_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Execution Id",
      "type": "string"
    },
    "inputs": {
      "$ref": "#/components/schemas/ExactSetAuthorityDocument"
    },
    "operation": {
      "$ref": "#/components/schemas/OperationIdentityDocument"
    },
    "retirement_grace_seconds": {
      "minimum": 0,
      "title": "Retirement Grace Seconds",
      "type": "integer"
    },
    "retirement_policy": {
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Retirement Policy",
      "type": "string"
    },
    "sealed_at": {
      "maxLength": 64,
      "minLength": 1,
      "title": "Sealed At",
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
  "title": "ProcessingClaimPlanDocument",
  "type": "object"
}
```
