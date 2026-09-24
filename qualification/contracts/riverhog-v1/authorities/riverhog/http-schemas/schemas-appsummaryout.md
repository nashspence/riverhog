# schemas: AppSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-appsummaryout:8140244a2e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3283097820"></a>

- <a id="s-5040e20914"></a>`type`: `"object"`
- <a id="s-2e16168791"></a>`additionalProperties`: `false`
- <a id="s-30f4fa1212"></a>`required`: `["name","keys","active_keys","last_used_at"]`
- <a id="s-0ab68a0e8e"></a>`title`: `"AppSummaryOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b581e74b72"></a>`active_keys` | yes | type="integer"; title="Active Keys" |  |
| <a id="s-ce23c1fe47"></a>`keys` | yes | type="integer"; title="Keys" |  |
| <a id="s-7cdb392047"></a>`last_used_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Last Used At" |  |
| <a id="s-514d86a11e"></a>`name` | yes | [ApplicationName](schemas-applicationname.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-4abe1d957f"></a>[field last_used_at · string value](#s-7cdb392047) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ApplicationName](schemas-applicationname.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-40a7fb4020"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-328f8f7be5"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppSummaryOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9154ed1f8f714a9de7b6b0fa0b777395587d7b4e11ba53ab10bfec5ea4869181 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "active_keys": {
      "title": "Active Keys",
      "type": "integer"
    },
    "keys": {
      "title": "Keys",
      "type": "integer"
    },
    "last_used_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Last Used At"
    },
    "name": {
      "$ref": "#/components/schemas/ApplicationName"
    }
  },
  "required": [
    "name",
    "keys",
    "active_keys",
    "last_used_at"
  ],
  "title": "AppSummaryOut",
  "type": "object"
}
```

</details>
