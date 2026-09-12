# schemas: ProvenanceTraceExternalStateReferenceItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-provenancetraceexternalstatereferenceitemout:a173c9863b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8a60d61b30"></a>
- <a id="s-46aa0a8a0c"></a>`title`: ProvenanceTraceExternalStateReferenceItemOut
- <a id="s-63e947ccd8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-82c2fe1e31"></a>`kind` | yes | type="string"; const="external_state_reference" |  |
| <a id="s-ac9f21d08d"></a>`reference` | yes | #/components/schemas/ProvenanceExternalStateReferenceOut |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ProvenanceExternalStateReferenceOut](schemas-provenanceexternalstatereferenceout.md)

## Governing policies

- <a id="pa-67af2420fb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceExternalStateReferenceItemOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14775eb0d4995fb0d8d72c90bed4aef8f5075c75e67f45ac5a8720eaf6eb1e67 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "kind": {
      "const": "external_state_reference",
      "title": "Kind",
      "type": "string"
    },
    "reference": {
      "$ref": "#/components/schemas/ProvenanceExternalStateReferenceOut"
    }
  },
  "required": [
    "kind",
    "reference"
  ],
  "title": "ProvenanceTraceExternalStateReferenceItemOut",
  "type": "object"
}
```
