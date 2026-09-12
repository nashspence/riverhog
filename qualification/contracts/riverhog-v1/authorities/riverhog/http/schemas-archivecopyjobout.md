# schemas: ArchiveCopyJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyjobout:4841696744 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7eb40bb862"></a>
- <a id="s-06125bc5d9"></a>`title`: ArchiveCopyJobOut
- <a id="s-e85e3e0f34"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f94ca18605"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-c99eec83e0"></a>`completed_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-ee02c17aac"></a>`destination_store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-17ef5e59ff"></a>`expires_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-79dd5dafe3"></a>`failure` | yes | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-c07f6332bc"></a>`initiated_by_app` | yes | anyOf=#/components/schemas/ApplicationName \| type="null" |  |
| <a id="s-0bcb097c2c"></a>`initiated_by_key_id` | yes | anyOf=#/components/schemas/ApplicationKeyId \| type="null" |  |
| <a id="s-88e645a59b"></a>`ready_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-69542ab9df"></a>`requested_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-7dd0ee0992"></a>`source_store` | yes | anyOf=#/components/schemas/ArchiveStoreName \| type="null" |  |
| <a id="s-9877b84351"></a>`state` | yes | #/components/schemas/ArchiveCopyState |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationKeyId](schemas-applicationkeyid.md)
- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: ArchiveCopyState](schemas-archivecopystate.md)
- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-de244aee67"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29dc4b874f692462c8d48922f23cd8d427114f94ade134c50fba648edc4b7427 -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "else": {
        "properties": {
          "completed_at": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "enum": [
              "completed",
              "canceled"
            ]
          }
        }
      },
      "then": {
        "properties": {
          "completed_at": {
            "type": "string"
          }
        }
      }
    },
    {
      "else": {
        "properties": {
          "failure": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "const": "failed"
          }
        }
      },
      "then": {
        "properties": {
          "failure": {
            "minLength": 1,
            "type": "string"
          }
        }
      }
    }
  ],
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "completed_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Completed At"
    },
    "destination_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "expires_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expires At"
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
    "initiated_by_app": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationName"
        },
        {
          "type": "null"
        }
      ]
    },
    "initiated_by_key_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationKeyId"
        },
        {
          "type": "null"
        }
      ]
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
    "requested_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Requested At"
    },
    "source_store": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveStoreName"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "$ref": "#/components/schemas/ArchiveCopyState"
    }
  },
  "required": [
    "collection_id",
    "source_store",
    "destination_store",
    "initiated_by_app",
    "initiated_by_key_id",
    "state",
    "requested_at",
    "ready_at",
    "expires_at",
    "completed_at",
    "failure"
  ],
  "title": "ArchiveCopyJobOut",
  "type": "object"
}
```
