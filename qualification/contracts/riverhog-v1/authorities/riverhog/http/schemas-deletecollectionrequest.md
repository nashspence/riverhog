# schemas: DeleteCollectionRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-deletecollectionrequest:512f679742 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-8dea5e6d7dea"></a>
- <a id="s-3bb2e798a0fd"></a>`title`: DeleteCollectionRequest
- <a id="s-f22afd69da99"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-238e9e80f884"></a>`challenge` | yes | type="string" |  |
| <a id="s-cad18f2c6c66"></a>`event_context` | no | anyOf=type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` \| type="null" |  |
| <a id="s-a4430dd637d9"></a>`retirement_claim_id` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b4c2f4d198cf"></a>field event_context · anyOf alternative 1 | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field event_context · anyOf alternative 1](#s-b4c2f4d198cf) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| <a id="s-0b341ccab2da"></a>field retirement_claim_id · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-cd9585634502"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-a8e9c3582136"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-4bb7ab5daead"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/DeleteCollectionRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c22cd89335617ccdcf6b1675cd71064d6573c80ac6515a180f5300e5b6b8c3a0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "challenge": {
      "title": "Challenge",
      "type": "string"
    },
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
    "retirement_claim_id": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Retirement Claim Id"
    }
  },
  "required": [
    "challenge"
  ],
  "title": "DeleteCollectionRequest",
  "type": "object"
}
```
