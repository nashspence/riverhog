# schemas: RetrievalPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalplanout:d94f1ee78a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-1d2ad45ba0f6"></a>
- <a id="s-4fd1ac3e21ba"></a>`title`: RetrievalPlanOut
- <a id="s-c35ffea3b67a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c266a5a49815"></a>`created_at` | yes | type="string" |  |
| <a id="s-e251cf38348d"></a>`etag` | yes | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-68c304646873"></a>`expires_at` | yes | type="string" |  |
| <a id="s-b41e85bc1227"></a>`failure` | yes | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-ddf8e0203e2f"></a>`file_count` | yes | type="integer"; minimum=1; maximum=10000 |  |
| <a id="s-ddadecec437f"></a>`format` | yes | type="string"; const="riverhog-retrieval-plan/v1" |  |
| <a id="s-715beca7a8c4"></a>`id` | yes | type="string" |  |
| <a id="s-6e0e6318d13f"></a>`lease_seconds` | yes | type="integer" |  |
| <a id="s-d30d97cea2a3"></a>`ready_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-c945c3441314"></a>`requires_restore` | yes | type="boolean" |  |
| <a id="s-00bdb5cadef4"></a>`restore_policy` | yes | type="string"; enum=["allow","never"] |  |
| <a id="s-9408ce999057"></a>`state` | yes | type="string"; enum=["planning","ready","consumed","expired","failed"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-1919cb4869fe"></a>field etag · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field file_count](#s-ddf8e0203e2f) | `value · schema-value · contract_max` | maximum=10000; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-4f108a8bbf69"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-0e8b0ac05246"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bfd67616847edef1e39584fbb8604ff423dc38761470242a99f5a66d86f28123 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "created_at": {
      "title": "Created At",
      "type": "string"
    },
    "etag": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Etag"
    },
    "expires_at": {
      "title": "Expires At",
      "type": "string"
    },
    "failure": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "file_count": {
      "maximum": 10000,
      "minimum": 1,
      "title": "File Count",
      "type": "integer"
    },
    "format": {
      "const": "riverhog-retrieval-plan/v1",
      "title": "Format",
      "type": "string"
    },
    "id": {
      "title": "Id",
      "type": "string"
    },
    "lease_seconds": {
      "title": "Lease Seconds",
      "type": "integer"
    },
    "ready_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Ready At"
    },
    "requires_restore": {
      "title": "Requires Restore",
      "type": "boolean"
    },
    "restore_policy": {
      "enum": [
        "allow",
        "never"
      ],
      "title": "Restore Policy",
      "type": "string"
    },
    "state": {
      "enum": [
        "planning",
        "ready",
        "consumed",
        "expired",
        "failed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "format",
    "id",
    "state",
    "created_at",
    "ready_at",
    "expires_at",
    "failure",
    "lease_seconds",
    "restore_policy",
    "requires_restore",
    "file_count",
    "etag"
  ],
  "title": "RetrievalPlanOut",
  "type": "object"
}
```
