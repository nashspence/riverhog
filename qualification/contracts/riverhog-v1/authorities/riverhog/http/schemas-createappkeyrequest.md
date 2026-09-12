# schemas: CreateAppKeyRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-createappkeyrequest:8d7c1a2cf8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8489af8a61"></a>
- <a id="s-3458023d0e"></a>`title`: CreateAppKeyRequest
- <a id="s-387c6d3823"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c99d7d5a34"></a>`access` | yes | #/components/schemas/ApplicationAccessGrantSet |  |
| <a id="s-cf3b3599cf"></a>`expires_in_seconds` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md)

## Governing policies

- <a id="pa-14a2fce8a5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateAppKeyRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2839386f89d3f9b0df156fcde75a23999719d77b217a25de451b7f40aa3c7125 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "access": {
      "$ref": "#/components/schemas/ApplicationAccessGrantSet"
    },
    "expires_in_seconds": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expires In Seconds"
    }
  },
  "required": [
    "access"
  ],
  "title": "CreateAppKeyRequest",
  "type": "object"
}
```
