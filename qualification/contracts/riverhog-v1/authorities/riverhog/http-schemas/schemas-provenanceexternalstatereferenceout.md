# schemas: ProvenanceExternalStateReferenceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-provenanceexternalstatereferenceout:20080be335 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-92a2f30f27"></a>

- <a id="s-b150d10b08"></a>`type`: `"object"`
- <a id="s-79e1c9d861"></a>`additionalProperties`: `false`
- <a id="s-a377cb19be"></a>`required`: `["from_journal_id","to_journal_id","state_id","entry_id","entry_json_sha256"]`
- <a id="s-c0aacb1f95"></a>`title`: `"ProvenanceExternalStateReferenceOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4ef91bcc8e"></a>`entry_id` | yes | [ProvenanceEntryId](schemas-provenanceentryid.md) |  |
| <a id="s-889c79b7b3"></a>`entry_json_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Entry Json Sha256" |  |
| <a id="s-451589e101"></a>`from_journal_id` | yes | [ProvenanceJournalId](schemas-provenancejournalid.md) |  |
| <a id="s-1913a68c52"></a>`state_id` | yes | [ProvenanceStateId](schemas-provenancestateid.md) |  |
| <a id="s-f5a4b84f5e"></a>`to_journal_id` | yes | [ProvenanceJournalId](schemas-provenancejournalid.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field entry_json_sha256](#s-889c79b7b3) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ProvenanceEntryId](schemas-provenanceentryid.md)
- [ProvenanceJournalId](schemas-provenancejournalid.md)
- [ProvenanceStateId](schemas-provenancestateid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a357d18df5"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-38eacfe75f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceExternalStateReferenceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
