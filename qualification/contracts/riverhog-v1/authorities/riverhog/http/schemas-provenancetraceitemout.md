# schemas: ProvenanceTraceItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancetraceitemout:a9f4a84490 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0b5dc86608f1"></a>
| Field | Shape |
|---|---|
| <a id="s-46d95c469442"></a>`discriminator` | additional keys=`mapping`, `propertyName` |
| <a id="s-a90b65adf907"></a>`oneOf` | items=#/components/schemas/ProvenanceTraceJournalItemOut \| #/components/schemas/ProvenanceTraceExternalStateReferenceItemOut |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ProvenanceTraceExternalStateReferenceItemOut](schemas-provenancetraceexternalstatereferenceitemout.md)
- [schemas: ProvenanceTraceJournalItemOut](schemas-provenancetracejournalitemout.md)

## Governing policies

- <a id="pa-fd12fe204bdb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceItemOut`

### Exact owned JSON

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
