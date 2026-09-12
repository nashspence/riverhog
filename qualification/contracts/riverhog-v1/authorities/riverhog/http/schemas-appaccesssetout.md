# schemas: AppAccessSetOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appaccesssetout:f7577e9305 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cc50975939f1"></a>
- <a id="s-c8eb7eeab2d1"></a>`title`: AppAccessSetOut
- <a id="s-d7ff4023f1ce"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-090ca1903c27"></a>`access` | yes | #/components/schemas/ApplicationAccessGrantSet |  |
| <a id="s-0c227a46cf2a"></a>`app` | yes | #/components/schemas/ApplicationName |  |
| <a id="s-b5f450fea36f"></a>`key_id` | yes | #/components/schemas/ApplicationKeyId |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md)
- [schemas: ApplicationKeyId](schemas-applicationkeyid.md)
- [schemas: ApplicationName](schemas-applicationname.md)

## Governing policies

- <a id="pa-e879a73710c0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessSetOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a2fe582b3f2d8be4000b822c697d94e382f3620ce1c2e6f75bafe2848bc27d9 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "access": {
      "$ref": "#/components/schemas/ApplicationAccessGrantSet"
    },
    "app": {
      "$ref": "#/components/schemas/ApplicationName"
    },
    "key_id": {
      "$ref": "#/components/schemas/ApplicationKeyId"
    }
  },
  "required": [
    "app",
    "key_id",
    "access"
  ],
  "title": "AppAccessSetOut",
  "type": "object"
}
```
