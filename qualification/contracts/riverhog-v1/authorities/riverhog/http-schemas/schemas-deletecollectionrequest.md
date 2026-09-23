# schemas: DeleteCollectionRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-deletecollectionrequest:add7710b62 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8dea5e6d7d"></a>

- <a id="s-f22afd69da"></a>`type`: `"object"`
- <a id="s-ae28c67701"></a>`additionalProperties`: `false`
- <a id="s-c0e1b58735"></a>`required`: `["challenge"]`
- <a id="s-3bb2e798a0"></a>`title`: `"DeleteCollectionRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-238e9e80f8"></a>`challenge` | yes | type="string"; title="Challenge" |  |
| <a id="s-cad18f2c6c"></a>`event_context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Event Context" |  |
| <a id="s-a4430dd637"></a>`retirement_claim_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Retirement Claim Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b4c2f4d198"></a>[field event_context · object value](#s-cad18f2c6c) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field event_context · object value](#s-b4c2f4d198) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| <a id="s-0b341ccab2"></a>[field retirement_claim_id · string value](#s-a4430dd637) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e89f80f9e3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-eb8051ee54"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-c9a881bf97"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/DeleteCollectionRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
