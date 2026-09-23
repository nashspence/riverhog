# schemas: CollectionUploadProvenanceJournalStatusDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadprovenancejournal-ba2d3d9a98:9cf20d3afa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5a9f227dd0"></a>

- <a id="s-9fa100d82d"></a>`type`: `"object"`
- <a id="s-4358631de9"></a>`additionalProperties`: `false`
- <a id="s-124c63a9ea"></a>`required`: `["journal_id","state","bytes","sha256","accepted_bytes"]`
- <a id="s-8e14aa9b6d"></a>`title`: `"CollectionUploadProvenanceJournalStatusDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-982c4304db"></a>`accepted_bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md) |  |
| <a id="s-9cb379acc2"></a>`bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-a21edea4a5"></a>`current_bytes` | no | anyOf=[([NonnegativeDecimal](schemas-nonnegativedecimal.md)); (type="null")] |  |
| <a id="s-ec0690f235"></a>`current_path` | no | anyOf=[(type="string"); (type="null")]; title="Current Path" |  |
| <a id="s-b72a4150cb"></a>`current_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Current Sha256" |  |
| <a id="s-60b9f44c31"></a>`current_state_id` | no | anyOf=[([ProvenanceStateId](schemas-provenancestateid.md)); (type="null")] |  |
| <a id="s-aa10735815"></a>`failure` | no | anyOf=[(type="string"); (type="null")]; title="Failure" |  |
| <a id="s-d2d8bb6a2c"></a>`journal_id` | yes | [ProvenanceJournalId](schemas-provenancejournalid.md) |  |
| <a id="s-f21af066db"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-ee287dd4a9"></a>`state` | yes | type="string"; enum=["accepting","validating","sealed","failed"]; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-14f4428270"></a>[field current_sha256 · string value](#s-b72a4150cb) | `length · characters · fixed` | shared above |
| [field sha256](#s-f21af066db) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [ProvenanceJournalId](schemas-provenancejournalid.md)
- [ProvenanceStateId](schemas-provenancestateid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3c86ecdfab"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-6a1f4deacc"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadProvenanceJournalStatusDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c992201f40dba3ac7f5e9844cd60fbe3b2dfdeadb685c1e777f26de27eed0d5d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "accepted_bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal"
    },
    "bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "current_bytes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/NonnegativeDecimal"
        },
        {
          "type": "null"
        }
      ]
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

</details>
