# schemas: SourceStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-sourcestatus:aa9830c291 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5dba419c36"></a>

- <a id="s-07683e6d9b"></a>`type`: `"object"`
- <a id="s-d8bed5f38a"></a>`additionalProperties`: `false`
- <a id="s-de1f851b1e"></a>`required`: `["id","ingest_source","claims","claim_bytes","close_mode","max_files","max_bytes","provenance","pending_claim_capacity","completion_failures","completion_failure_capacity","oldest_completion_failure"]`
- <a id="s-6afe95c9fb"></a>`title`: `"SourceStatus"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6bec35a8fc"></a>`claim_bytes` | yes | type="integer"; minimum=0; title="Claim Bytes" |  |
| <a id="s-c50d4163cc"></a>`claims` | yes | type="integer"; minimum=0; title="Claims" |  |
| <a id="s-4c974cc5ce"></a>`close_mode` | yes | type="string"; enum=["stable","explicit-flush"]; title="Close Mode" |  |
| <a id="s-e8676a9b37"></a>`completion_failure_capacity` | yes | type="integer"; minimum=1; title="Completion Failure Capacity" |  |
| <a id="s-40d171fcf5"></a>`completion_failures` | yes | type="integer"; minimum=0; title="Completion Failures" |  |
| <a id="s-ea0b615809"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-4036ede3b2"></a>`ingest_source` | yes | type="string"; title="Ingest Source" |  |
| <a id="s-4890e4fdee"></a>`max_bytes` | yes | type="integer"; minimum=1; title="Max Bytes" |  |
| <a id="s-993fedbe71"></a>`max_files` | yes | type="integer"; minimum=1; title="Max Files" |  |
| <a id="s-5946a41fc1"></a>`oldest_completion_failure` | yes | anyOf=[([CompletionFailureStatus](schemas-completionfailurestatus.md)); (type="null")] |  |
| <a id="s-f931d136ba"></a>`pending_claim_capacity` | yes | type="integer"; minimum=1; title="Pending Claim Capacity" |  |
| <a id="s-865a2cc4c8"></a>`provenance` | yes | type="string"; enum=["capture","omit"]; title="Provenance" |  |

## Maintained corroboration

### Referenced contract elements

- [CompletionFailureStatus](schemas-completionfailurestatus.md)

## Governing policies

- <a id="pa-6528d2b09a"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/SourceStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5727f6b3b94b7946c7542e7e55b5c895e11839ca06af35ab8722515bf4ea2882 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_bytes": {
      "minimum": 0,
      "title": "Claim Bytes",
      "type": "integer"
    },
    "claims": {
      "minimum": 0,
      "title": "Claims",
      "type": "integer"
    },
    "close_mode": {
      "enum": [
        "stable",
        "explicit-flush"
      ],
      "title": "Close Mode",
      "type": "string"
    },
    "completion_failure_capacity": {
      "minimum": 1,
      "title": "Completion Failure Capacity",
      "type": "integer"
    },
    "completion_failures": {
      "minimum": 0,
      "title": "Completion Failures",
      "type": "integer"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "ingest_source": {
      "title": "Ingest Source",
      "type": "string"
    },
    "max_bytes": {
      "minimum": 1,
      "title": "Max Bytes",
      "type": "integer"
    },
    "max_files": {
      "minimum": 1,
      "title": "Max Files",
      "type": "integer"
    },
    "oldest_completion_failure": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CompletionFailureStatus"
        },
        {
          "type": "null"
        }
      ]
    },
    "pending_claim_capacity": {
      "minimum": 1,
      "title": "Pending Claim Capacity",
      "type": "integer"
    },
    "provenance": {
      "enum": [
        "capture",
        "omit"
      ],
      "title": "Provenance",
      "type": "string"
    }
  },
  "required": [
    "id",
    "ingest_source",
    "claims",
    "claim_bytes",
    "close_mode",
    "max_files",
    "max_bytes",
    "provenance",
    "pending_claim_capacity",
    "completion_failures",
    "completion_failure_capacity",
    "oldest_completion_failure"
  ],
  "title": "SourceStatus",
  "type": "object"
}
```

</details>
