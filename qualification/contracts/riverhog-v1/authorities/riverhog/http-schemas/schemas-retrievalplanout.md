# schemas: RetrievalPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalplanout:3463d780f0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-1d2ad45ba0"></a>

- <a id="s-c35ffea3b6"></a>`type`: `"object"`
- <a id="s-0b417a3305"></a>`additionalProperties`: `false`
- <a id="s-06224d9133"></a>`required`: `["format","id","state","created_at","ready_at","expires_at","failure","lease_seconds","restore_policy","requires_restore","file_count","etag"]`
- <a id="s-4fd1ac3e21"></a>`title`: `"RetrievalPlanOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c266a5a498"></a>`created_at` | yes | type="string"; title="Created At" |  |
| <a id="s-e251cf3834"></a>`etag` | yes | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Etag" |  |
| <a id="s-68c3046468"></a>`expires_at` | yes | type="string"; title="Expires At" |  |
| <a id="s-b41e85bc12"></a>`failure` | yes | anyOf=[(type="string"; minLength=1); (type="null")]; title="Failure" |  |
| <a id="s-ddf8e0203e"></a>`file_count` | yes | type="integer"; minimum=1; maximum=10000; title="File Count" |  |
| <a id="s-ddadecec43"></a>`format` | yes | type="string"; const="riverhog-retrieval-plan/v1"; title="Format" |  |
| <a id="s-715beca7a8"></a>`id` | yes | type="string"; title="Id" |  |
| <a id="s-6e0e6318d1"></a>`lease_seconds` | yes | type="integer"; title="Lease Seconds" |  |
| <a id="s-d30d97cea2"></a>`ready_at` | yes | anyOf=[(type="string"); (type="null")]; title="Ready At" |  |
| <a id="s-c945c34413"></a>`requires_restore` | yes | type="boolean"; title="Requires Restore" |  |
| <a id="s-00bdb5cade"></a>`restore_policy` | yes | type="string"; enum=["allow","never"]; title="Restore Policy" |  |
| <a id="s-9408ce9990"></a>`state` | yes | type="string"; enum=["planning","ready","consumed","expired","failed"]; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-1919cb4869"></a>[field etag · string value](#s-e251cf3834) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field file_count](#s-ddf8e0203e) | `value · schema-value · contract_max` | maximum=10000; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-84a80dfd42"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c3c9831b9d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
