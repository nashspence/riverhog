# schemas: ClaimRegisteredEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-claimregisteredevent:baa67dbe91 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-2976c793d2"></a>

- <a id="s-6ffc9ee629"></a>`type`: `"object"`
- <a id="s-035f4bfefa"></a>`additionalProperties`: `false`
- <a id="s-09f65843ed"></a>`required`: `["id","type","subject","occurred_at","payload"]`
- <a id="s-3a9e78c989"></a>`title`: `"ClaimRegisteredEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7c8e17831c"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-2d9984ebf7"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-aa8d27a7bb"></a>`payload` | yes | [ClaimRegisteredPayload](schemas-claimregisteredpayload.md) |  |
| <a id="s-1e06bf431a"></a>`subject` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Subject" |  |
| <a id="s-1d23bb88a5"></a>`type` | yes | type="string"; const="io.riverhog.ftp_spool.claim.registered"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-2d9984ebf7) | `length · characters · fixed` | maximum=30; minimum=30 |
| [field subject](#s-1e06bf431a) | `length · characters · fixed` | maximum=64; minimum=64; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [ClaimRegisteredPayload](schemas-claimregisteredpayload.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a71d432bf3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-34da4015eb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/ClaimRegisteredEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12b2510ffc7c3f89f573f23dc208ba9e7fc59c70d7613a755dd4ad0839c32593 -->

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
      "$ref": "#/components/schemas/ClaimRegisteredPayload"
    },
    "subject": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Subject",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.ftp_spool.claim.registered",
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
  "title": "ClaimRegisteredEvent",
  "type": "object"
}
```

</details>
