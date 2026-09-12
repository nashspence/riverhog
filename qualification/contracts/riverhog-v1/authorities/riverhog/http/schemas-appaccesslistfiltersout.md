# schemas: AppAccessListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appaccesslistfiltersout:80c05b15d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c18ac0714e"></a>
- <a id="s-2a0e135c28"></a>`title`: AppAccessListFiltersOut
- <a id="s-095198a992"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00a576f7d9"></a>`active` | yes | anyOf=type="boolean" \| type="null" |  |
| <a id="s-f9c07ac7d4"></a>`app` | yes | anyOf=#/components/schemas/ApplicationName \| type="null" |  |
| <a id="s-58bc6e1be8"></a>`key_id` | yes | anyOf=#/components/schemas/ApplicationKeyId \| type="null" |  |
| <a id="s-9a0a41b672"></a>`permission` | yes | anyOf=#/components/schemas/ApplicationPermission \| type="null" |  |
| <a id="s-546a9b227f"></a>`resource` | yes | anyOf=#/components/schemas/ApplicationResource \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationKeyId](schemas-applicationkeyid.md)
- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: ApplicationPermission](schemas-applicationpermission.md)
- [schemas: ApplicationResource](schemas-applicationresource.md)

## Governing policies

- <a id="pa-d56979450c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessListFiltersOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 58c3b0affb66a528725d95691c3bcf296127dcbceab2530c96991cf42714fb86 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "active": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "title": "Active"
    },
    "app": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationName"
        },
        {
          "type": "null"
        }
      ]
    },
    "key_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationKeyId"
        },
        {
          "type": "null"
        }
      ]
    },
    "permission": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationPermission"
        },
        {
          "type": "null"
        }
      ]
    },
    "resource": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationResource"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "app",
    "key_id",
    "permission",
    "resource",
    "active"
  ],
  "title": "AppAccessListFiltersOut",
  "type": "object"
}
```
