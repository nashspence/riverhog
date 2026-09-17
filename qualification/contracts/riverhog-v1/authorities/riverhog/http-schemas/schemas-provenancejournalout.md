# schemas: ProvenanceJournalOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-provenancejournalout:67c7c5988d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-72c52c1f9b"></a>

- <a id="s-866ba6766d"></a>`type`: `"object"`
- <a id="s-02ab422923"></a>`additionalProperties`: `false`
- <a id="s-c61f3a1a7e"></a>`required`: `["journal_id","bytes","sha256","entries","current_state_id","current_path","current_bytes","current_sha256","agent_count","entity_counts"]`
- <a id="s-f5a80d3d54"></a>`title`: `"ProvenanceJournalOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ed25b7e83"></a>`agent_count` | yes | type="integer"; minimum=0; title="Agent Count" |  |
| <a id="s-f408563044"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-482f2e7361"></a>`current_bytes` | yes | type="integer"; minimum=0; title="Current Bytes" |  |
| <a id="s-6215516aa0"></a>`current_path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-2fed6651de"></a>`current_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Current Sha256" |  |
| <a id="s-39da7b375a"></a>`current_state_id` | yes | [ProvenanceStateId](schemas-provenancestateid.md) |  |
| <a id="s-5f305a524f"></a>`entity_counts` | yes | type="object"; additionalProperties=(type="integer"); title="Entity Counts" |  |
| <a id="s-904aca57ef"></a>`entries` | yes | type="integer"; minimum=1; title="Entries" |  |
| <a id="s-0590071dee"></a>`journal_id` | yes | [ProvenanceJournalId](schemas-provenancejournalid.md) |  |
| <a id="s-3f5ee56d63"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

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

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [ProvenanceJournalId](schemas-provenancejournalid.md)
- [ProvenanceStateId](schemas-provenancestateid.md)

## Governing policies

- <a id="pa-d2f102e6ab"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-7f11ee05b1"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-ffdf591ee7"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceJournalOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
