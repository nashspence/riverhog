# schemas: ArchiveDownloadAllowanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivedownloadallowanceout:d3a06c5643 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-46f521de79"></a>

- <a id="s-7270dcd7af"></a>`type`: `"object"`
- <a id="s-b12d9f6c6b"></a>`additionalProperties`: `false`
- <a id="s-c853bcb9d0"></a>`required`: `["store","state","month_started_at","resets_at","allowance_bytes","safety_buffer_bytes","effective_limit_bytes","accounted_bytes","reserved_bytes","remaining_bytes"]`
- <a id="s-4519ce4635"></a>`title`: `"ArchiveDownloadAllowanceOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ef21559a1"></a>`accounted_bytes` | yes | type="integer"; title="Accounted Bytes" |  |
| <a id="s-e79165f0f2"></a>`allowance_bytes` | yes | type="integer"; title="Allowance Bytes" |  |
| <a id="s-9ff4a596cd"></a>`effective_limit_bytes` | yes | type="integer"; title="Effective Limit Bytes" |  |
| <a id="s-099dbcfcba"></a>`month_started_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Month Started At" |  |
| <a id="s-3c36967233"></a>`remaining_bytes` | yes | type="integer"; title="Remaining Bytes" |  |
| <a id="s-2b5c28bc14"></a>`reserved_bytes` | yes | type="integer"; title="Reserved Bytes" |  |
| <a id="s-138f1ddbcc"></a>`resets_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Resets At" |  |
| <a id="s-3b2f6d1df2"></a>`safety_buffer_bytes` | yes | type="integer"; title="Safety Buffer Bytes" |  |
| <a id="s-38ef97275a"></a>`state` | yes | type="string"; enum=["open","closed"]; title="State" |  |
| <a id="s-51a26ac5cc"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field month_started_at](#s-099dbcfcba) | `length · characters · fixed` | shared above |
| [field resets_at](#s-138f1ddbcc) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a8c49fdb9f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-16d666f068"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveDownloadAllowanceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c91a48067ff7eb986b133688c3c7086be5405f0130d4d75c505e7042d629703 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "accounted_bytes": {
      "title": "Accounted Bytes",
      "type": "integer"
    },
    "allowance_bytes": {
      "title": "Allowance Bytes",
      "type": "integer"
    },
    "effective_limit_bytes": {
      "title": "Effective Limit Bytes",
      "type": "integer"
    },
    "month_started_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Month Started At",
      "type": "string"
    },
    "remaining_bytes": {
      "title": "Remaining Bytes",
      "type": "integer"
    },
    "reserved_bytes": {
      "title": "Reserved Bytes",
      "type": "integer"
    },
    "resets_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Resets At",
      "type": "string"
    },
    "safety_buffer_bytes": {
      "title": "Safety Buffer Bytes",
      "type": "integer"
    },
    "state": {
      "enum": [
        "open",
        "closed"
      ],
      "title": "State",
      "type": "string"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    }
  },
  "required": [
    "store",
    "state",
    "month_started_at",
    "resets_at",
    "allowance_bytes",
    "safety_buffer_bytes",
    "effective_limit_bytes",
    "accounted_bytes",
    "reserved_bytes",
    "remaining_bytes"
  ],
  "title": "ArchiveDownloadAllowanceOut",
  "type": "object"
}
```

</details>
