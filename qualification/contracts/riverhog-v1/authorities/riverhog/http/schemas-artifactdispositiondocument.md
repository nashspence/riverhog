# schemas: ArtifactDispositionDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositiondocument:9ebd40c640 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionDocument`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactDispositionFailureDocument](schemas-artifactdispositionfailuredocument.md)
- [schemas: ArtifactDispositionInputDocument](schemas-artifactdispositioninputdocument.md)

## Contract summary

- `title`: ArtifactDispositionDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `failure` | no | object (1 fields) |  |
| `input` | yes | #/components/schemas/ArtifactDispositionInputDocument |  |
| `status` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: edfafa82144c8be20244c30934ad87c7fee86c5237da936cff0d58ef40a5a48c -->

```json
{
  "additionalProperties": false,
  "oneOf": [
    {
      "properties": {
        "failure": {
          "type": "null"
        },
        "status": {
          "enum": [
            "transformed",
            "preserved"
          ]
        }
      }
    },
    {
      "properties": {
        "failure": {
          "type": "object"
        },
        "status": {
          "enum": [
            "omitted",
            "rejected"
          ]
        }
      },
      "required": [
        "failure"
      ]
    }
  ],
  "properties": {
    "failure": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArtifactDispositionFailureDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "input": {
      "$ref": "#/components/schemas/ArtifactDispositionInputDocument"
    },
    "status": {
      "enum": [
        "transformed",
        "preserved",
        "omitted",
        "rejected"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "input",
    "status"
  ],
  "title": "ArtifactDispositionDocument",
  "type": "object"
}
```
