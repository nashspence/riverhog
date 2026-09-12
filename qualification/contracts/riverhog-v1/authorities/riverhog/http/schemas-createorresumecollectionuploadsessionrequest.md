# schemas: CreateOrResumeCollectionUploadSessionRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-createorresumecollectionuploadsessionrequest:b45d70029f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-751d9669fa10"></a>
- <a id="s-0ade5d291c67"></a>`title`: CreateOrResumeCollectionUploadSessionRequest
- <a id="s-93feacf501fb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1edb4de31934"></a>`archive_store` | no | anyOf=#/components/schemas/ArchiveStoreName \| type="null" |  |
| <a id="s-f6c4e0c61a3d"></a>`custody_mode` | no | type="string"; enum=["producer-retained","custody-transfer"] |  |
| <a id="s-1f95c5607e01"></a>`description` | no | anyOf=#/components/schemas/CollectionDescription \| type="null" |  |
| <a id="s-818938036516"></a>`event_context` | no | anyOf=type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` \| type="null" |  |
| <a id="s-11b776ca6b56"></a>`idempotency_key` | yes | type="string"; minLength=1; maxLength=200; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| <a id="s-aa7adb94668b"></a>`ingest_source` | no | anyOf=type="string" \| type="null" |  |
| <a id="s-9c8b7f769b65"></a>`initial_tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8fbd974a3d10"></a>`provenance_mode` | no | type="string"; enum=["captured","omitted"] |  |
| <a id="s-cdc66d8038b0"></a>`provenance_omission_reason` | no | anyOf=type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" \| type="null" |  |
| <a id="s-d7947f281e50"></a>`tags` | no | type="array"; maxItems=100; items=(#/components/schemas/CollectionTag); additional keys=`x-riverhog-extent` |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

Shared facts for every subject below: maximum=100; minimum=null; progression={"progression":"repeat-request"}; reason="bounded-upload-staging-step; collection-tag-set-is-unbounded"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field tags](#s-d7947f281e50) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-8b8b6b1136d5"></a>field event_context · anyOf alternative 1 | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field event_context · anyOf alternative 1](#s-8b8b6b1136d5) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field idempotency_key](#s-11b776ca6b56) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [field initial_tag_set_identity](#s-9c8b7f769b65) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionDescription](schemas-collectiondescription.md)
- [schemas: CollectionTag](schemas-collectiontag.md)

## Governing policies

- <a id="pa-7cb7a2f4b1d2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-97c7c3a63444"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)
- <a id="pa-c9abb620d48d"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-3401b28636bb"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateOrResumeCollectionUploadSessionRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc6291eecb861dbb701cf14027b2438c68f9e11cbe218c22b48f32eec8c478a8 -->

```json
{
  "additionalProperties": false,
  "oneOf": [
    {
      "properties": {
        "provenance_mode": {
          "const": "captured"
        },
        "provenance_omission_reason": {
          "type": "null"
        }
      }
    },
    {
      "properties": {
        "provenance_mode": {
          "const": "omitted"
        },
        "provenance_omission_reason": {
          "type": "string"
        }
      },
      "required": [
        "provenance_mode",
        "provenance_omission_reason"
      ]
    }
  ],
  "properties": {
    "archive_store": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveStoreName"
        },
        {
          "type": "null"
        }
      ]
    },
    "custody_mode": {
      "default": "producer-retained",
      "enum": [
        "producer-retained",
        "custody-transfer"
      ],
      "title": "Custody Mode",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionDescription"
        },
        {
          "type": "null"
        }
      ]
    },
    "event_context": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 4096,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-lifecycle-event-context"
          }
        },
        {
          "type": "null"
        }
      ],
      "title": "Event Context"
    },
    "idempotency_key": {
      "maxLength": 200,
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Idempotency Key",
      "type": "string"
    },
    "ingest_source": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Ingest Source"
    },
    "initial_tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Initial Tag Set Identity",
      "type": "string"
    },
    "provenance_mode": {
      "default": "captured",
      "enum": [
        "captured",
        "omitted"
      ],
      "title": "Provenance Mode",
      "type": "string"
    },
    "provenance_omission_reason": {
      "anyOf": [
        {
          "minLength": 1,
          "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Provenance Omission Reason"
    },
    "tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "maxItems": 100,
      "title": "Tags",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "repeat-request",
        "reason": "bounded-upload-staging-step; collection-tag-set-is-unbounded"
      }
    }
  },
  "required": [
    "idempotency_key",
    "initial_tag_set_identity"
  ],
  "title": "CreateOrResumeCollectionUploadSessionRequest",
  "type": "object"
}
```
