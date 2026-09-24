# schemas: CustodyReadyEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-custodyreadyevent:f377878858 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b27f7d7e20"></a>

- <a id="s-04f3d48142"></a>`type`: `"object"`
- <a id="s-f1fb642877"></a>`additionalProperties`: `false`
- <a id="s-136f0c95ce"></a>`required`: `["id","type","subject","occurred_at","payload"]`
- <a id="s-26054c0ac1"></a>`title`: `"CustodyReadyEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f65c9292b"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-dd080322b6"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-d466004303"></a>`payload` | yes | [CustodyReadyPayload](schemas-custodyreadypayload.md) |  |
| <a id="s-b79b353ab5"></a>`subject` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Subject" |  |
| <a id="s-9521bc272d"></a>`type` | yes | type="string"; const="io.riverhog.ftp_spool.claim.custody_ready"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-dd080322b6) | `length · characters · fixed` | maximum=30; minimum=30 |
| [field subject](#s-b79b353ab5) | `length · characters · fixed` | maximum=64; minimum=64; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [CustodyReadyPayload](schemas-custodyreadypayload.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2fa9cfbc4d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3565026435"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/CustodyReadyEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cc80dca712f0a2a6bb6d54884ea55d55bfb459e2fa0bcb9652dad587b3f0cd8 -->

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
      "$ref": "#/components/schemas/CustodyReadyPayload"
    },
    "subject": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Subject",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.ftp_spool.claim.custody_ready",
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
  "title": "CustodyReadyEvent",
  "type": "object"
}
```

</details>
