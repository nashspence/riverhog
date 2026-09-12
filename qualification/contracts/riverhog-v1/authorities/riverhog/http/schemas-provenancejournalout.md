# schemas: ProvenanceJournalOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancejournalout:557e77cc2b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-72c52c1f9b"></a>
- <a id="s-f5a80d3d54"></a>`title`: ProvenanceJournalOut
- <a id="s-866ba6766d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ed25b7e83"></a>`agent_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-f408563044"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-482f2e7361"></a>`current_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-6215516aa0"></a>`current_path` | yes | #/components/schemas/CanonicalRelPath |  |
| <a id="s-2fed6651de"></a>`current_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-39da7b375a"></a>`current_state_id` | yes | #/components/schemas/ProvenanceStateId |  |
| <a id="s-5f305a524f"></a>`entity_counts` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-904aca57ef"></a>`entries` | yes | type="integer"; minimum=1 |  |
| <a id="s-0590071dee"></a>`journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| <a id="s-3f5ee56d63"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-f408563044) | `value · schema-value · operational_policy` | shared above |
| [field entity_counts](#s-5f305a524f) | `cardinality · entries · operational_policy` | shared above |
| [field entries](#s-904aca57ef) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field current_sha256](#s-2fed6651de) | `length · characters · fixed` | shared above |
| [field sha256](#s-3f5ee56d63) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CanonicalRelPath](schemas-canonicalrelpath.md)
- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)
- [schemas: ProvenanceStateId](schemas-provenancestateid.md)

## Governing policies

- <a id="pa-27c40a33fe"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-ccf1372761"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-3d3d47bbf3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceJournalOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b4d5bd09f7cf8ce6972fbf494eb28c6511c569fa3f79630616a0eb4b11f622f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "agent_count": {
      "minimum": 0,
      "title": "Agent Count",
      "type": "integer"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "current_bytes": {
      "minimum": 0,
      "title": "Current Bytes",
      "type": "integer"
    },
    "current_path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "current_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Current Sha256",
      "type": "string"
    },
    "current_state_id": {
      "$ref": "#/components/schemas/ProvenanceStateId"
    },
    "entity_counts": {
      "additionalProperties": {
        "type": "integer"
      },
      "title": "Entity Counts",
      "type": "object"
    },
    "entries": {
      "minimum": 1,
      "title": "Entries",
      "type": "integer"
    },
    "journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "journal_id",
    "bytes",
    "sha256",
    "entries",
    "current_state_id",
    "current_path",
    "current_bytes",
    "current_sha256",
    "agent_count",
    "entity_counts"
  ],
  "title": "ProvenanceJournalOut",
  "type": "object"
}
```
