# schemas: ProvenanceTraceJournalItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-provenancetracejournalitemout:3a2f8af6df -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-fb2e560d2b"></a>

- <a id="s-8584794adf"></a>`type`: `"object"`
- <a id="s-e08d773bcf"></a>`additionalProperties`: `false`
- <a id="s-69749ad8d4"></a>`required`: `["kind","journal"]`
- <a id="s-d6ee5f6a8d"></a>`title`: `"ProvenanceTraceJournalItemOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e5d6e0d334"></a>`journal` | yes | [ProvenanceJournalOut](schemas-provenancejournalout.md) |  |
| <a id="s-08fd7d5fdc"></a>`kind` | yes | type="string"; const="journal"; title="Kind" |  |

## Maintained corroboration

### Referenced contract elements

- [ProvenanceJournalOut](schemas-provenancejournalout.md)

## Governing policies

- <a id="pa-7c4be577ad"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceJournalItemOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
