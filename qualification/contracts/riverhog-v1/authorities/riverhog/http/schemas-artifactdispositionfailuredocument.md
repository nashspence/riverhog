# schemas: ArtifactDispositionFailureDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionfailuredocument:2812e8a6ef -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionFailureDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=500, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: ArtifactDispositionFailureDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `code` | yes | string |  |
| `message` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb1686770d101da119a837092e0db98866f1ce367c5cec0c865061898dcc3642 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Code",
      "type": "string"
    },
    "message": {
      "maxLength": 500,
      "minLength": 1,
      "title": "Message",
      "type": "string"
    }
  },
  "required": [
    "code",
    "message"
  ],
  "title": "ArtifactDispositionFailureDocument",
  "type": "object"
}
```
