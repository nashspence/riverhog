# schemas: _FileProvenanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-fileprovenanceout:75b44e6184 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-60336dfbc1"></a>
| Field | Shape |
|---|---|
| <a id="s-bf98f839a8"></a>`anyOf` | items=#/components/schemas/CapturedCollectionFileProvenanceOut \| #/components/schemas/OmittedCollectionFileProvenanceOut |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CapturedCollectionFileProvenanceOut](schemas-capturedcollectionfileprovenanceout.md)
- [schemas: OmittedCollectionFileProvenanceOut](schemas-omittedcollectionfileprovenanceout.md)

## Governing policies

- <a id="pa-5f1db401c1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/_FileProvenanceOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1bc71fe1eca29d749d7950ecb0a31e22bd1a68d42d7a613e0193edaa0006f17 -->

```json
{
  "anyOf": [
    {
      "$ref": "#/components/schemas/CapturedCollectionFileProvenanceOut"
    },
    {
      "$ref": "#/components/schemas/OmittedCollectionFileProvenanceOut"
    }
  ]
}
```
