# schemas: AttemptFailedPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-attemptfailedpayload:c4ed4e8764 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b9659c19ce"></a>

- <a id="s-feb0ac5a2f"></a>`type`: `"object"`
- <a id="s-6b67f05367"></a>`additionalProperties`: `false`
- <a id="s-fe323da445"></a>`required`: `["source_id","claim_id","source_event_id","error_type"]`
- <a id="s-a187db74e4"></a>`title`: `"AttemptFailedPayload"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-572fd61eeb"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |  |
| <a id="s-8a59f265ba"></a>`error_type` | yes | type="string"; maxLength=160; minLength=1; title="Error Type" |  |
| <a id="s-8a00a1f059"></a>`source_event_id` | yes | type="string"; maxLength=300; minLength=1; title="Source Event Id" |  |
| <a id="s-239d0cd944"></a>`source_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Source Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-572fd61eeb) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field error_type](#s-8a59f265ba) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field source_event_id](#s-8a00a1f059) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-da594a6a85"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-892bdd15fb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/AttemptFailedPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de6b452e1be1d821d976ea3b372b9835bd4d94ad5c38408a5a4af288f0e13c0f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "error_type": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Error Type",
      "type": "string"
    },
    "source_event_id": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Source Event Id",
      "type": "string"
    },
    "source_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
      "title": "Source Id",
      "type": "string"
    }
  },
  "required": [
    "source_id",
    "claim_id",
    "source_event_id",
    "error_type"
  ],
  "title": "AttemptFailedPayload",
  "type": "object"
}
```

</details>
