# schemas: ProcessingClaimFiltersDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimfiltersdocument:347988e701 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a70300dbcf"></a>
- <a id="s-fd946a5579"></a>`title`: ProcessingClaimFiltersDocument
- <a id="s-0fb9297f4f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6a7a54d76b"></a>`state` | no | anyOf=type="string"; enum=["active","settled","retiring","abandoned","released"] \| type="null" |  |

## Governing policies

- <a id="pa-8097a83f49"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimFiltersDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59b4d8bb229a8c43d763c982eebd067a71e6bccd1c8851ee169948435abc1158 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "state": {
      "anyOf": [
        {
          "enum": [
            "active",
            "settled",
            "retiring",
            "abandoned",
            "released"
          ],
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "State"
    }
  },
  "title": "ProcessingClaimFiltersDocument",
  "type": "object"
}
```
