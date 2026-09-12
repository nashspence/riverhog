# schemas: ArchiveDownloadAllowanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivedownloadallowanceout:7189efe81a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-46f521de7969"></a>
- <a id="s-4519ce4635c0"></a>`title`: ArchiveDownloadAllowanceOut
- <a id="s-7270dcd7afb4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ef21559a15a"></a>`accounted_bytes` | yes | type="integer" |  |
| <a id="s-e79165f0f276"></a>`allowance_bytes` | yes | type="integer" |  |
| <a id="s-9ff4a596cd16"></a>`effective_limit_bytes` | yes | type="integer" |  |
| <a id="s-099dbcfcba66"></a>`month_started_at` | yes | type="string" |  |
| <a id="s-3c3696723310"></a>`remaining_bytes` | yes | type="integer" |  |
| <a id="s-2b5c28bc1444"></a>`reserved_bytes` | yes | type="integer" |  |
| <a id="s-138f1ddbccc3"></a>`resets_at` | yes | type="string" |  |
| <a id="s-3b2f6d1df241"></a>`safety_buffer_bytes` | yes | type="integer" |  |
| <a id="s-38ef97275a1e"></a>`state` | yes | type="string"; enum=["open","closed"] |  |
| <a id="s-51a26ac5cc56"></a>`store` | yes | #/components/schemas/ArchiveStoreName |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- <a id="pa-0c428bb06200"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveDownloadAllowanceOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bf5085d23cac01726e33f1c0aaab7af69c8b4af270cffb72c5387ff400b329a -->

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
