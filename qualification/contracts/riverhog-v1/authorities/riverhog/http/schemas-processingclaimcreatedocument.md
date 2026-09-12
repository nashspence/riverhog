# schemas: ProcessingClaimCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimcreatedocument:5abc67569d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

<a id="s-305a17ea50"></a>
- <a id="s-50eec0ea4e"></a>`title`: ProcessingClaimCreateDocument
- <a id="s-9c4f4f21f4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3601c46110"></a>`lease_seconds` | no | type="integer"; minimum=30; maximum=86400 |  |
| <a id="s-227587b793"></a>`purpose` | no | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-fcd9a436ed"></a>`work_document` | yes | type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-8334d12b08"></a>`work_document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-935ed0da9d"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work_document](#s-fcd9a436ed) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field lease_seconds](#s-3601c46110) | `value · schema-value · contract_max` | maximum=86400; minimum=30; reason="schema-maximum" |
| [field purpose](#s-227587b793) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field work_document](#s-fcd9a436ed) | `encoded-size · bytes · contract_max` | maximum=4194304; reason="bounded-work-document-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field work_document_sha256](#s-8334d12b08) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field work_id](#s-935ed0da9d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-7d2f43c983"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-7102c7196f"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-52fac10733"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimCreateDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f81f54843c68bd43b662283b523cf02085ec0e8d673cd66cdb5201223ea00a6 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "lease_seconds": {
      "default": 1800,
      "maximum": 86400,
      "minimum": 30,
      "title": "Lease Seconds",
      "type": "integer"
    },
    "purpose": {
      "default": "collection-work/v1",
      "maxLength": 160,
      "minLength": 1,
      "title": "Purpose",
      "type": "string"
    },
    "work_document": {
      "additionalProperties": true,
      "title": "Work Document",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 4194304,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-work-document-envelope"
      }
    },
    "work_document_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Document Sha256",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "work_id",
    "work_document",
    "work_document_sha256"
  ],
  "title": "ProcessingClaimCreateDocument",
  "type": "object"
}
```
