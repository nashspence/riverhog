# schemas: ProvenanceExternalStateReferenceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenanceexternalstatereferenceout:27323ab20c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-92a2f30f27"></a>
- <a id="s-c0aacb1f95"></a>`title`: ProvenanceExternalStateReferenceOut
- <a id="s-b150d10b08"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4ef91bcc8e"></a>`entry_id` | yes | #/components/schemas/ProvenanceEntryId |  |
| <a id="s-889c79b7b3"></a>`entry_json_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-451589e101"></a>`from_journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| <a id="s-1913a68c52"></a>`state_id` | yes | #/components/schemas/ProvenanceStateId |  |
| <a id="s-f5a4b84f5e"></a>`to_journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field entry_json_sha256](#s-889c79b7b3) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ProvenanceEntryId](schemas-provenanceentryid.md)
- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)
- [schemas: ProvenanceStateId](schemas-provenancestateid.md)

## Governing policies

- <a id="pa-1cd077740c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-c138c2518a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceExternalStateReferenceOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0be22da19b0a8f539bd3883f9540fdeb932c4ee27c46a109f44a17090f8e9865 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "entry_id": {
      "$ref": "#/components/schemas/ProvenanceEntryId"
    },
    "entry_json_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Entry Json Sha256",
      "type": "string"
    },
    "from_journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
    },
    "state_id": {
      "$ref": "#/components/schemas/ProvenanceStateId"
    },
    "to_journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
    }
  },
  "required": [
    "from_journal_id",
    "to_journal_id",
    "state_id",
    "entry_id",
    "entry_json_sha256"
  ],
  "title": "ProvenanceExternalStateReferenceOut",
  "type": "object"
}
```
