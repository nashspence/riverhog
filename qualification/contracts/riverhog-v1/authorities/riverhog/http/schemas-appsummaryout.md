# schemas: AppSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appsummaryout:d221fc0283 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-328309782088"></a>
- <a id="s-0ab68a0e8e01"></a>`title`: AppSummaryOut
- <a id="s-5040e209144a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b581e74b722b"></a>`active_keys` | yes | type="integer" |  |
| <a id="s-ce23c1fe4727"></a>`keys` | yes | type="integer" |  |
| <a id="s-7cdb39204708"></a>`last_used_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-514d86a11e21"></a>`name` | yes | #/components/schemas/ApplicationName |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationName](schemas-applicationname.md)

## Governing policies

- <a id="pa-b6847062d1b5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppSummaryOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a648910ef6f83e51e08d9cec0bcc14ba96ab319b0811a1203f9260ac13a404cb -->

```json
{
  "additionalProperties": false,
  "properties": {
    "active_keys": {
      "title": "Active Keys",
      "type": "integer"
    },
    "keys": {
      "title": "Keys",
      "type": "integer"
    },
    "last_used_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Last Used At"
    },
    "name": {
      "$ref": "#/components/schemas/ApplicationName"
    }
  },
  "required": [
    "name",
    "keys",
    "active_keys",
    "last_used_at"
  ],
  "title": "AppSummaryOut",
  "type": "object"
}
```
