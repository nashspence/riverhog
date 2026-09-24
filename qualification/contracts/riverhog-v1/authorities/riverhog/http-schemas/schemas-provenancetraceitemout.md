# schemas: ProvenanceTraceItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-provenancetraceitemout:7169bfb0bb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-0b5dc86608"></a>

- <a id="s-46d95c4694"></a>`discriminator`: `{"mapping":{"external_state_reference":"#/components/schemas/ProvenanceTraceExternalStateReferenceItemOut","journal":"#/components/schemas/ProvenanceTraceJournalItemOut"},"propertyName":"kind"}`

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-83fad83917"></a>1 | [ProvenanceTraceJournalItemOut](schemas-provenancetracejournalitemout.md) |
| <a id="s-bf79926edb"></a>2 | [ProvenanceTraceExternalStateReferenceItemOut](schemas-provenancetraceexternalstatereferenceitemout.md) |

## Maintained corroboration

### Referenced contract elements

- [ProvenanceTraceExternalStateReferenceItemOut](schemas-provenancetraceexternalstatereferenceitemout.md)
- [ProvenanceTraceJournalItemOut](schemas-provenancetracejournalitemout.md)

## Governing policies

- <a id="pa-3bf64dce95"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceItemOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87420641aa260811cc4582e164bbd05cee21465ad23b0e05d84c4feef7205ddc -->

```json
{
  "discriminator": {
    "mapping": {
      "external_state_reference": "#/components/schemas/ProvenanceTraceExternalStateReferenceItemOut",
      "journal": "#/components/schemas/ProvenanceTraceJournalItemOut"
    },
    "propertyName": "kind"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/ProvenanceTraceJournalItemOut"
    },
    {
      "$ref": "#/components/schemas/ProvenanceTraceExternalStateReferenceItemOut"
    }
  ]
}
```

</details>
