# schemas: CapturedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-capturedfileprovenancebinding:620384966f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e192eeba17"></a>

- <a id="s-a478ef17b4"></a>`type`: `"object"`
- <a id="s-550779fed5"></a>`additionalProperties`: `false`
- <a id="s-3afb7e5c22"></a>`required`: `["journal_id","current_state_id","status"]`
- <a id="s-980ea6478f"></a>`title`: `"CapturedFileProvenanceBinding"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35d0cc218c"></a>`current_state_id` | yes | [ProvenanceStateId](schemas-provenancestateid.md) |  |
| <a id="s-7ba525f46a"></a>`journal_id` | yes | [ProvenanceJournalId](schemas-provenancejournalid.md) |  |
| <a id="s-b3e65c66e3"></a>`status` | yes | type="string"; const="captured"; title="Status" |  |

## Maintained corroboration

### Referenced contract elements

- [ProvenanceJournalId](schemas-provenancejournalid.md)
- [ProvenanceStateId](schemas-provenancestateid.md)

## Governing policies

- <a id="pa-8299da14d9"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CapturedFileProvenanceBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf3ce6fcaf00f930e4e5a3c9173e96f05af02be3acf636affeb667fd35e2e11b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "current_state_id": {
      "$ref": "#/components/schemas/ProvenanceStateId"
    },
    "journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
    },
    "status": {
      "const": "captured",
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "journal_id",
    "current_state_id",
    "status"
  ],
  "title": "CapturedFileProvenanceBinding",
  "type": "object"
}
```

</details>
