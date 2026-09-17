# schemas: CreateOrResumeCollectionUploadSessionRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-createorresumecollectionuploadsessionrequest:bc77a5d01f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-751d9669fa"></a>

- <a id="s-93feacf501"></a>`type`: `"object"`
- <a id="s-1077d1d9d4"></a>`additionalProperties`: `false`
- <a id="s-ca263ee794"></a>`required`: `["idempotency_key","initial_tag_set_identity"]`
- <a id="s-0ade5d291c"></a>`title`: `"CreateOrResumeCollectionUploadSessionRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1edb4de319"></a>`archive_store` | no | anyOf=[([ArchiveStoreName](schemas-archivestorename.md)); (type="null")] |  |
| <a id="s-f6c4e0c61a"></a>`custody_mode` | no | type="string"; enum=["producer-retained","custody-transfer"]; default="producer-retained"; title="Custody Mode" |  |
| <a id="s-1f95c5607e"></a>`description` | no | anyOf=[([CollectionDescription](schemas-collectiondescription.md)); (type="null")] |  |
| <a id="s-8189380365"></a>`event_context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Event Context" |  |
| <a id="s-11b776ca6b"></a>`idempotency_key` | yes | type="string"; maxLength=200; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"; title="Idempotency Key" |  |
| <a id="s-aa7adb9466"></a>`ingest_source` | no | anyOf=[(type="string"); (type="null")]; title="Ingest Source" |  |
| <a id="s-9c8b7f769b"></a>`initial_tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Initial Tag Set Identity" |  |
| <a id="s-8fbd974a3d"></a>`provenance_mode` | no | type="string"; enum=["captured","omitted"]; default="captured"; title="Provenance Mode" |  |
| <a id="s-cdc66d8038"></a>`provenance_omission_reason` | no | anyOf=[(type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"); (type="null")]; title="Provenance Omission Reason" |  |
| <a id="s-d7947f281e"></a>`tags` | no | type="array"; items=([CollectionTag](schemas-collectiontag.md)); maxItems=100; title="Tags"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"repeat-request","reason":"bounded-upload-staging-step; collection-tag-set-is-unbounded"} |  |

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `oneOf` alternative 1](#s-2fe533a22f) |
| 2 | [See `oneOf` alternative 2](#s-57076eb34e) |

### <a id="s-2fe533a22f"></a>`oneOf` alternative 1


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e4f6f2982"></a>`provenance_mode` | no | const="captured" |  |
| <a id="s-288c139b2c"></a>`provenance_omission_reason` | no | type="null" |  |

### <a id="s-57076eb34e"></a>`oneOf` alternative 2

- <a id="s-ead4de5d59"></a>`required`: `["provenance_mode","provenance_omission_reason"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-57314536d2"></a>`provenance_mode` | yes | const="omitted" |  |
| <a id="s-37f238e7c6"></a>`provenance_omission_reason` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=100; minimum=null; progression={"progression":"repeat-request"}; reason="bounded-upload-staging-step; collection-tag-set-is-unbounded"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field tags](#s-d7947f281e) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-8b8b6b1136"></a>[field event_context · object value](#s-8189380365) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field event_context · object value](#s-8b8b6b1136) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field idempotency_key](#s-11b776ca6b) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [field initial_tag_set_identity](#s-9c8b7f769b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594).

Exact evidence groups for this contract element:

- [riverhog-upload-tag-staging-progression/v1](../../../evidence/qualifications/riverhog-upload-tag-staging-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionDescription](schemas-collectiondescription.md)
- [CollectionTag](schemas-collectiontag.md)

## Governing policies

- <a id="pa-44e7b0bd6a"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-cbe225f614"></a>[extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)
- <a id="pa-6e8cc7c8db"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-90fb155bba"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateOrResumeCollectionUploadSessionRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
