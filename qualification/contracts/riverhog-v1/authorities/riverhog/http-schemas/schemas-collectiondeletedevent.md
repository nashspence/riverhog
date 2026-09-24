# schemas: CollectionDeletedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiondeletedevent:e36b3b3918 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-120961c294"></a>

- <a id="s-8e56cefa42"></a>`type`: `"object"`
- <a id="s-ceb9afa059"></a>`additionalProperties`: `false`
- <a id="s-a919cd0e30"></a>`required`: `["id","type","occurred_at","payload"]`
- <a id="s-ac54d472d8"></a>`title`: `"CollectionDeletedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7dff09b524"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-6af6a24677"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-b1a6849c37"></a>`payload` | yes | [CollectionDeletedData](schemas-collectiondeleteddata.md) |  |
| <a id="s-30bba64930"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-2f5c894029"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.collection.deleted"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-6af6a24677) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionDeletedData](schemas-collectiondeleteddata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c2b0471b07"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e8c049bdd4"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b65b108cc02d796fa37cec610e829195ca16525ad552924a11016f214b3629c -->

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
      "$ref": "#/components/schemas/CollectionDeletedData"
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
      "const": "io.riverhog.riverhog.collection.deleted",
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
  "title": "CollectionDeletedEvent",
  "type": "object"
}
```

</details>
