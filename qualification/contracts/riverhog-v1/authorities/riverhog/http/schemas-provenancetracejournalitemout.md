# schemas: ProvenanceTraceJournalItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancetracejournalitemout:670a5f832c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-fb2e560d2bce"></a>
- <a id="s-d6ee5f6a8da5"></a>`title`: ProvenanceTraceJournalItemOut
- <a id="s-8584794adfa7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e5d6e0d334c6"></a>`journal` | yes | #/components/schemas/ProvenanceJournalOut |  |
| <a id="s-08fd7d5fdc4a"></a>`kind` | yes | type="string"; const="journal" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ProvenanceJournalOut](schemas-provenancejournalout.md)

## Governing policies

- <a id="pa-dc68bc1c057c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceJournalItemOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60175e4be5e1b36c1cfca848086a59e5d4cca9f8a0223bc4e4d3c01d2fc1aaad -->

```json
{
  "additionalProperties": false,
  "properties": {
    "journal": {
      "$ref": "#/components/schemas/ProvenanceJournalOut"
    },
    "kind": {
      "const": "journal",
      "title": "Kind",
      "type": "string"
    }
  },
  "required": [
    "kind",
    "journal"
  ],
  "title": "ProvenanceTraceJournalItemOut",
  "type": "object"
}
```
