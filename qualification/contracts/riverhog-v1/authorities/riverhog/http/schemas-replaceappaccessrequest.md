# schemas: ReplaceAppAccessRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-replaceappaccessrequest:55bfbf2277 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-380c3f60fd"></a>
- <a id="s-8b3ae7d02f"></a>`title`: ReplaceAppAccessRequest
- <a id="s-7f816904c8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3698bbca24"></a>`access` | yes | #/components/schemas/ApplicationAccessGrantSet |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md)

## Governing policies

- <a id="pa-be221e75e5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ReplaceAppAccessRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95fe088d01d861de5cd6e4f12e8a679f80043db02021062e83bd52673b2cf245 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "access": {
      "$ref": "#/components/schemas/ApplicationAccessGrantSet"
    }
  },
  "required": [
    "access"
  ],
  "title": "ReplaceAppAccessRequest",
  "type": "object"
}
```
