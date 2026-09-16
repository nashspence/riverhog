# schemas: RetrievalPlanRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalplanrequest:7b28933b22 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4b2e57f624"></a>

- <a id="s-4a8dc3ad10"></a>`type`: `"object"`
- <a id="s-5c5041a734"></a>`additionalProperties`: `false`
- <a id="s-b58681aa7d"></a>`required`: `["files","idempotency_key"]`
- <a id="s-8aa7907ec0"></a>`title`: `"RetrievalPlanRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92156c5164"></a>`files` | yes | type="array"; items=(#/components/schemas/RetrievalFileReferenceDocument); maxItems=10000; minItems=1; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"multiple-retrieval-jobs","reason":"bounded-retrieval-work-request"} |  |
| <a id="s-1281abc044"></a>`idempotency_key` | yes | type="string"; maxLength=200; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| <a id="s-ca9836474f"></a>`lease_seconds` | no | anyOf=(type="integer"; minimum=1) \| (type="null") |  |
| <a id="s-f33de244d2"></a>`restore_policy` | no | type="string"; enum=["allow","never"]; default="allow" |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=10000; minimum=1; progression={"progression":"multiple-retrieval-jobs"}; reason="bounded-retrieval-work-request"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-92156c5164) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=200; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field idempotency_key](#s-1281abc044) | `length · characters · contract_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-retrieval-work-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-bff92ce2bd)

## Maintained corroboration

### Referenced contract dossiers

- [RetrievalFileReferenceDocument](schemas-retrievalfilereferencedocument.md)

## Governing policies

- <a id="pa-b9024a3331"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-389764c3ef"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)
- <a id="pa-a864915f4e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0713d37a8808e79acb34c07987edcd1d422626671a7dcf49e3a452cbbaf30d0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "files": {
      "items": {
        "$ref": "#/components/schemas/RetrievalFileReferenceDocument"
      },
      "maxItems": 10000,
      "minItems": 1,
      "title": "Files",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "multiple-retrieval-jobs",
        "reason": "bounded-retrieval-work-request"
      }
    },
    "idempotency_key": {
      "maxLength": 200,
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Idempotency Key",
      "type": "string"
    },
    "lease_seconds": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Lease Seconds"
    },
    "restore_policy": {
      "default": "allow",
      "enum": [
        "allow",
        "never"
      ],
      "title": "Restore Policy",
      "type": "string"
    }
  },
  "required": [
    "files",
    "idempotency_key"
  ],
  "title": "RetrievalPlanRequest",
  "type": "object"
}
```

</details>
