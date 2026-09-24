# schemas: CustodyReadyPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-custodyreadypayload:c1a958afcf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4725380b27"></a>

- <a id="s-908fd3de3c"></a>`type`: `"object"`
- <a id="s-5c11bd3fc1"></a>`additionalProperties`: `false`
- <a id="s-48cd015223"></a>`required`: `["source_id","claim_id","source_event_id","file_count","bytes"]`
- <a id="s-c8897c13c2"></a>`title`: `"CustodyReadyPayload"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ce18533623"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-253357f5c6"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |  |
| <a id="s-7749bf2b31"></a>`file_count` | yes | type="integer"; minimum=1; title="File Count" |  |
| <a id="s-5745edb3de"></a>`source_event_id` | yes | type="string"; maxLength=300; minLength=1; title="Source Event Id" |  |
| <a id="s-7f9be45f1a"></a>`source_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Source Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-ftp-spool"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-ce18533623) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-253357f5c6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field source_event_id](#s-5745edb3de) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b7a693910a"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c4b0e2c78d"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-308b1ac6f4"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/CustodyReadyPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55d8ac60db50f7182ac115809ec6a02f4aba994bda523a0fb9981b1872302046 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "file_count": {
      "minimum": 1,
      "title": "File Count",
      "type": "integer"
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
    "file_count",
    "bytes"
  ],
  "title": "CustodyReadyPayload",
  "type": "object"
}
```

</details>
