# schemas: ArchiveCopyJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobout:ef86dbefe2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7eb40bb862"></a>

- <a id="s-e85e3e0f34"></a>`type`: `"object"`
- <a id="s-11abcce352"></a>`additionalProperties`: `false`
- <a id="s-d8044f6042"></a>`required`: `["collection_id","source_store","destination_store","use_cache","initiated_by_app","initiated_by_key_id","state","requested_at","ready_at","expires_at","finished_at","failure"]`
- <a id="s-06125bc5d9"></a>`title`: `"ArchiveCopyJobOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f94ca18605"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-ee02c17aac"></a>`destination_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-17ef5e59ff"></a>`expires_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Expires At" |  |
| <a id="s-79dd5dafe3"></a>`failure` | yes | anyOf=[(type="string"; minLength=1); (type="null")]; title="Failure" |  |
| <a id="s-42f7be85cb"></a>`finished_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Finished At" |  |
| <a id="s-c07f6332bc"></a>`initiated_by_app` | yes | [ApplicationName](schemas-applicationname.md) |  |
| <a id="s-0bcb097c2c"></a>`initiated_by_key_id` | yes | anyOf=[([ApplicationKeyId](schemas-applicationkeyid.md)); (type="null")] |  |
| <a id="s-88e645a59b"></a>`ready_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Ready At" |  |
| <a id="s-69542ab9df"></a>`requested_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Requested At" |  |
| <a id="s-7dd0ee0992"></a>`source_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-9877b84351"></a>`state` | yes | [ArchiveCopyJobState](schemas-archivecopyjobstate.md) |  |
| <a id="s-c6a6e38c82"></a>`use_cache` | yes | type="boolean"; title="Use Cache" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-fbea60a442"></a>1 | properties={state: (enum=["completed","failed","canceled"])} | properties={finished_at: (type="string")} | properties={finished_at: (type="null")} |
| <a id="s-ab9983dcd9"></a>2 | properties={state: (const="failed")} | properties={failure: (type="string"; minLength=1)} | properties={failure: (type="null")} |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-0355aecb3d"></a>[field expires_at · string value](#s-17ef5e59ff) | `length · characters · fixed` | shared above |
| <a id="s-dd6ca32ef7"></a>[field finished_at · string value](#s-42f7be85cb) | `length · characters · fixed` | shared above |
| <a id="s-968e84def1"></a>[field ready_at · string value](#s-88e645a59b) | `length · characters · fixed` | shared above |
| [field requested_at](#s-69542ab9df) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ApplicationKeyId](schemas-applicationkeyid.md)
- [ApplicationName](schemas-applicationname.md)
- [ArchiveCopyJobState](schemas-archivecopyjobstate.md)
- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c71050b819"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-fbd184f3ae"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3bcb6833b7fe722a77dbdd935f0394d803e153be4e8f4ce16cdac70743ec49d8 -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "else": {
        "properties": {
          "finished_at": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "enum": [
              "completed",
              "failed",
              "canceled"
            ]
          }
        }
      },
      "then": {
        "properties": {
          "finished_at": {
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
    "destination_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "expires_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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
    "finished_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Finished At"
    },
    "initiated_by_app": {
      "$ref": "#/components/schemas/ApplicationName"
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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Ready At"
    },
    "requested_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Requested At",
      "type": "string"
    },
    "source_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "state": {
      "$ref": "#/components/schemas/ArchiveCopyJobState"
    },
    "use_cache": {
      "title": "Use Cache",
      "type": "boolean"
    }
  },
  "required": [
    "collection_id",
    "source_store",
    "destination_store",
    "use_cache",
    "initiated_by_app",
    "initiated_by_key_id",
    "state",
    "requested_at",
    "ready_at",
    "expires_at",
    "finished_at",
    "failure"
  ],
  "title": "ArchiveCopyJobOut",
  "type": "object"
}
```

</details>
