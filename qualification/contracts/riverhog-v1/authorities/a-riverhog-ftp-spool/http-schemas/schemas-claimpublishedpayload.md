# schemas: ClaimPublishedPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-claimpublishedpayload:36679997f5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e75c3cb028"></a>

- <a id="s-2f26d58047"></a>`type`: `"object"`
- <a id="s-c4170aa246"></a>`additionalProperties`: `false`
- <a id="s-5f5d21fcea"></a>`required`: `["source_id","claim_id","source_event_id","collection_id","archive_root_sha256","content_identity"]`
- <a id="s-10b403af95"></a>`title`: `"ClaimPublishedPayload"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9653c78411"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-97198f4d88"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |  |
| <a id="s-adad1906f9"></a>`collection_id` | yes | type="string"; pattern="^[1-9][0-9]*$"; title="Collection Id" |  |
| <a id="s-6866bea01a"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-c0e4b02507"></a>`source_event_id` | yes | type="string"; maxLength=300; minLength=1; title="Source Event Id" |  |
| <a id="s-787ffe9629"></a>`source_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Source Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-9653c78411) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field claim_id](#s-97198f4d88) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field content_identity](#s-6866bea01a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field source_event_id](#s-c0e4b02507) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f6b518a223"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-926643e561"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/ClaimPublishedPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0274c9f118d43fb8ad3331eab75c50278f07a4ebba7056c44d5266f17d1b60b7 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Root Sha256",
      "type": "string"
    },
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "collection_id": {
      "pattern": "^[1-9][0-9]*$",
      "title": "Collection Id",
      "type": "string"
    },
    "content_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Content Identity",
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
    "collection_id",
    "archive_root_sha256",
    "content_identity"
  ],
  "title": "ClaimPublishedPayload",
  "type": "object"
}
```

</details>
