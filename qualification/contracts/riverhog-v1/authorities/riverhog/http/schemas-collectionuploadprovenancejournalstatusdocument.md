# schemas: CollectionUploadProvenanceJournalStatusDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadprovenancejournal-ba2d3d9a98:4790d84516 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-5a9f227dd0"></a>
- <a id="s-8e14aa9b6d"></a>`title`: CollectionUploadProvenanceJournalStatusDocument
- <a id="s-9fa100d82d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-982c4304db"></a>`accepted_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-9cb379acc2"></a>`bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-a21edea4a5"></a>`current_bytes` | no | anyOf=type="integer"; minimum=0 \| type="null" |  |
| <a id="s-ec0690f235"></a>`current_path` | no | anyOf=type="string" \| type="null" |  |
| <a id="s-b72a4150cb"></a>`current_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-60b9f44c31"></a>`current_state_id` | no | anyOf=#/components/schemas/ProvenanceStateId \| type="null" |  |
| <a id="s-aa10735815"></a>`failure` | no | anyOf=type="string" \| type="null" |  |
| <a id="s-d2d8bb6a2c"></a>`journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| <a id="s-f21af066db"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ee287dd4a9"></a>`state` | yes | type="string"; enum=["accepting","validating","sealed","failed"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-9cb379acc2) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-14f4428270"></a>[field current_sha256 · string value](#s-b72a4150cb) | `length · characters · fixed` | shared above |
| [field sha256](#s-f21af066db) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)
- [schemas: ProvenanceStateId](schemas-provenancestateid.md)

## Governing policies

- <a id="pa-62071002eb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-8b27d81174"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-3b5ec80b54"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadProvenanceJournalStatusDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c7fc6e75fbd9fde5ed7dcae5ff41845ece61b2feec55575dc2a37b709bdc269 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "accepted_bytes": {
      "minimum": 0,
      "title": "Accepted Bytes",
      "type": "integer"
    },
    "bytes": {
      "minimum": 1,
      "title": "Bytes",
      "type": "integer"
    },
    "current_bytes": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Current Bytes"
    },
    "current_path": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Current Path"
    },
    "current_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Current Sha256"
    },
    "current_state_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ProvenanceStateId"
        },
        {
          "type": "null"
        }
      ]
    },
    "failure": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    },
    "state": {
      "enum": [
        "accepting",
        "validating",
        "sealed",
        "failed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "journal_id",
    "state",
    "bytes",
    "sha256",
    "accepted_bytes"
  ],
  "title": "CollectionUploadProvenanceJournalStatusDocument",
  "type": "object"
}
```
