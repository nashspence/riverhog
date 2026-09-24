# schemas: RetrievalExpiredEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalexpiredevent:b1edcdbe97 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e102080776"></a>

- <a id="s-83e3b5fdb2"></a>`type`: `"object"`
- <a id="s-6caf13bba3"></a>`additionalProperties`: `false`
- <a id="s-ef4a07f699"></a>`required`: `["id","type","occurred_at","payload"]`
- <a id="s-2e46c614ff"></a>`title`: `"RetrievalExpiredEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1f4f2d7d08"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-53444ad165"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-93f9764d46"></a>`payload` | yes | [RetrievalExpiredData](schemas-retrievalexpireddata.md) |  |
| <a id="s-4dadef44a7"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-4d9f982b0f"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.expired"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-53444ad165) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [RetrievalExpiredData](schemas-retrievalexpireddata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c1d1d1afb8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-7a922bc58c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalExpiredEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd3fbe28d688b8a8d9802fb20205990b2042909f16a2c3e2ca05fedf4b6d7a93 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "id": {
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "occurred_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Occurred At",
      "type": "string"
    },
    "payload": {
      "$ref": "#/components/schemas/RetrievalExpiredData"
    },
    "subject": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Subject"
    },
    "type": {
      "const": "io.riverhog.riverhog.retrieval.expired",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "type",
    "occurred_at",
    "payload"
  ],
  "title": "RetrievalExpiredEvent",
  "type": "object"
}
```

</details>
