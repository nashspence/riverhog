# schemas: CreateRetrievalJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-createretrievaljobrequest:cd962825a2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-2ae82e7b3a"></a>

- <a id="s-127793fd4d"></a>`type`: `"object"`
- <a id="s-d73f722c07"></a>`additionalProperties`: `false`
- <a id="s-91cb295276"></a>`required`: `["plan_id"]`
- <a id="s-8c6846117d"></a>`title`: `"CreateRetrievalJobRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7437f6cbd1"></a>`event_context` | no | anyOf=(type="object"; additionalProperties=true; x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}) \| (type="null") |  |
| <a id="s-56cc2e0453"></a>`plan_id` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-971557cb41"></a>[field event_context · object value](#s-7437f6cbd1) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field event_context · object value](#s-971557cb41) | `encoded-size · bytes · contract_max` | shared above |

## Governing policies

- <a id="pa-1f54a1db26"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-96eea3ae6d"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-391e92bd00"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateRetrievalJobRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8be23595a98af525a48d70ccd78c62c429aef2a8b7e0676da7c4ab1a2ddca0b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "event_context": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 4096,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-lifecycle-event-context"
          }
        },
        {
          "type": "null"
        }
      ],
      "title": "Event Context"
    },
    "plan_id": {
      "title": "Plan Id",
      "type": "string"
    }
  },
  "required": [
    "plan_id"
  ],
  "title": "CreateRetrievalJobRequest",
  "type": "object"
}
```

</details>
