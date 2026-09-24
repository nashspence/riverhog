# schemas: BranchSetAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-branchsetadmittedevent:a5faa53f02 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-751c9ba6ad"></a>

- <a id="s-90f8fe8621"></a>`type`: `"object"`
- <a id="s-9271a5b14f"></a>`additionalProperties`: `false`
- <a id="s-2a5f25323f"></a>`required`: `["id","type","subject","occurred_at","payload"]`
- <a id="s-81b45141f4"></a>`title`: `"BranchSetAdmittedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-db8b0a166a"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-bf3d3b408f"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-9acd6b4632"></a>`payload` | yes | [BranchSetAdmittedEventData](schemas-branchsetadmittedeventdata.md) |  |
| <a id="s-5021293ee9"></a>`subject` | yes | type="string"; minLength=1; title="Subject" |  |
| <a id="s-c424b8e8e3"></a>`type` | yes | type="string"; const="io.riverhog.stove0.branch-set.admitted"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-bf3d3b408f) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [BranchSetAdmittedEventData](schemas-branchsetadmittedeventdata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d6085c991b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-05111c4b87"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetAdmittedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 472482b2ddd753f6cff81370a4d5cf89af9b2fc83f4f3540b9bf312b9a7f352e -->

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
      "$ref": "#/components/schemas/BranchSetAdmittedEventData"
    },
    "subject": {
      "minLength": 1,
      "title": "Subject",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.stove0.branch-set.admitted",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "type",
    "subject",
    "occurred_at",
    "payload"
  ],
  "title": "BranchSetAdmittedEvent",
  "type": "object"
}
```

</details>
