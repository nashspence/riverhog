# schemas: ArtifactDispositionDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositiondocument:9ebd40c640 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ArtifactDispositionDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `failure` | no | anyOf=#/components/schemas/ArtifactDispositionFailureDocument \| type="null" |  |
| `input` | yes | #/components/schemas/ArtifactDispositionInputDocument |  |
| `status` | yes | type="string"; enum=["transformed","preserved","omitted","rejected"] |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionFailureDocument](schemas-artifactdispositionfailuredocument.md)
- [schemas: ArtifactDispositionInputDocument](schemas-artifactdispositioninputdocument.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionDocument`

### Exact owned JSON

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
