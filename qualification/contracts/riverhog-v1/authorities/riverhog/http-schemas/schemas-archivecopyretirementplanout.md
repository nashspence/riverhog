# schemas: ArchiveCopyRetirementPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyretirementplanout:073bedc909 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-05bde79c4b"></a>

- <a id="s-86305dce22"></a>`type`: `"object"`
- <a id="s-b920650ee4"></a>`additionalProperties`: `false`
- <a id="s-2e90ad56f9"></a>`required`: `["status","collection_id","store","warning","expires_at","challenge","target_copy","retained_copies","terminal_retrieval_job_count","blockers","verification_note","billing_note"]`
- <a id="s-77e7f36434"></a>`title`: `"ArchiveCopyRetirementPlanOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-662b46ed5a"></a>`billing_note` | yes | type="string"; title="Billing Note" |  |
| <a id="s-3bb1c53f61"></a>`blockers` | yes | type="array"; items=(type="string"); title="Blockers" |  |
| <a id="s-a8bf1cfd6c"></a>`challenge` | yes | anyOf=[(type="string"); (type="null")]; title="Challenge" |  |
| <a id="s-b3e346a57e"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-22bb7f1875"></a>`expires_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Expires At" |  |
| <a id="s-3b329cb5f9"></a>`retained_copies` | yes | type="array"; items=([ArchiveCopyRetirementRetainedOut](schemas-archivecopyretirementretainedout.md)); title="Retained Copies" |  |
| <a id="s-bc86b383bf"></a>`status` | yes | type="string"; enum=["ready","blocked","retiring"]; title="Status" |  |
| <a id="s-ccd5b75997"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-1bc1feaa61"></a>`target_copy` | yes | [ArchiveCopyRetirementTargetOut](schemas-archivecopyretirementtargetout.md) |  |
| <a id="s-59e028b38b"></a>`terminal_retrieval_job_count` | yes | type="integer"; title="Terminal Retrieval Job Count" |  |
| <a id="s-0e9fe4a515"></a>`verification_note` | yes | type="string"; title="Verification Note" |  |
| <a id="s-3902eea4d7"></a>`warning` | yes | type="string"; title="Warning" |  |

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `oneOf` alternative 1](#s-178cb4c49f) |
| 2 | [See `oneOf` alternative 2](#s-bfe171a84c) |

### <a id="s-178cb4c49f"></a>`oneOf` alternative 1


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-191e6ff8d3"></a>`blockers` | no | minItems=1 |  |
| <a id="s-f234572c77"></a>`challenge` | no | type="null" |  |
| <a id="s-58ffc09adb"></a>`status` | no | const="blocked" |  |

### <a id="s-bfe171a84c"></a>`oneOf` alternative 2


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-82a7ddf46a"></a>`blockers` | no | maxItems=0 |  |
| <a id="s-453077a665"></a>`challenge` | no | type="string"; minLength=1 |  |
| <a id="s-9d526db619"></a>`status` | no | enum=["ready","retiring"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [oneOf alternative 1 · field blockers](#s-191e6ff8d3) | `cardinality · items · operational_policy` | shared above |
| [field blockers](#s-3bb1c53f61) | `cardinality · items · operational_policy` | shared above |
| [field retained_copies](#s-3b329cb5f9) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [oneOf alternative 2 · field blockers](#s-82a7ddf46a) | `cardinality · items · contract_max` | maximum=0; reason="state-conditioned-empty-set" |
| [field expires_at](#s-22bb7f1875) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |

## Maintained corroboration

### Referenced contract elements

- [ArchiveCopyRetirementRetainedOut](schemas-archivecopyretirementretainedout.md)
- [ArchiveCopyRetirementTargetOut](schemas-archivecopyretirementtargetout.md)
- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-80614c9394"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-10f3583f07"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-d1ae0a62b6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementPlanOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acba47a96474d6b719fb0f0a15656782f6fd74efd5c943003d6e1cd6c015700f -->

```json
{
  "additionalProperties": false,
  "oneOf": [
    {
      "properties": {
        "blockers": {
          "minItems": 1
        },
        "challenge": {
          "type": "null"
        },
        "status": {
          "const": "blocked"
        }
      }
    },
    {
      "properties": {
        "blockers": {
          "maxItems": 0
        },
        "challenge": {
          "minLength": 1,
          "type": "string"
        },
        "status": {
          "enum": [
            "ready",
            "retiring"
          ]
        }
      }
    }
  ],
  "properties": {
    "billing_note": {
      "title": "Billing Note",
      "type": "string"
    },
    "blockers": {
      "items": {
        "type": "string"
      },
      "title": "Blockers",
      "type": "array"
    },
    "challenge": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Challenge"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "expires_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Expires At",
      "type": "string"
    },
    "retained_copies": {
      "items": {
        "$ref": "#/components/schemas/ArchiveCopyRetirementRetainedOut"
      },
      "title": "Retained Copies",
      "type": "array"
    },
    "status": {
      "enum": [
        "ready",
        "blocked",
        "retiring"
      ],
      "title": "Status",
      "type": "string"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "target_copy": {
      "$ref": "#/components/schemas/ArchiveCopyRetirementTargetOut"
    },
    "terminal_retrieval_job_count": {
      "title": "Terminal Retrieval Job Count",
      "type": "integer"
    },
    "verification_note": {
      "title": "Verification Note",
      "type": "string"
    },
    "warning": {
      "title": "Warning",
      "type": "string"
    }
  },
  "required": [
    "status",
    "collection_id",
    "store",
    "warning",
    "expires_at",
    "challenge",
    "target_copy",
    "retained_copies",
    "terminal_retrieval_job_count",
    "blockers",
    "verification_note",
    "billing_note"
  ],
  "title": "ArchiveCopyRetirementPlanOut",
  "type": "object"
}
```

</details>
