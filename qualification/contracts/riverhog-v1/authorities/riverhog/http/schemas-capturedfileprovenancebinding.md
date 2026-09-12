# schemas: CapturedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-capturedfileprovenancebinding:61c9a53c26 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e192eeba17"></a>
- <a id="s-980ea6478f"></a>`title`: CapturedFileProvenanceBinding
- <a id="s-a478ef17b4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35d0cc218c"></a>`current_state_id` | yes | #/components/schemas/ProvenanceStateId |  |
| <a id="s-7ba525f46a"></a>`journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| <a id="s-b3e65c66e3"></a>`status` | yes | type="string"; const="captured" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)
- [schemas: ProvenanceStateId](schemas-provenancestateid.md)

## Governing policies

- <a id="pa-fd9ddb94f6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CapturedFileProvenanceBinding`

### Exact owned JSON

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
