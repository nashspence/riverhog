# schemas: CatalogSyncCheckpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-catalogsynccheckpoint:a0ea4d55c4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-9ddd462b8c"></a>
- <a id="s-20a428ce84"></a>`title`: CatalogSyncCheckpoint
- <a id="s-b913c0e64e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83cd2c8930"></a>`authorization_view_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dc231f2428"></a>`catalog_cursor` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-cc40445d90"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1" |  |
| <a id="s-e8505dafc9"></a>`source_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field authorization_view_identity](#s-83cd2c8930) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field catalog_cursor](#s-dc231f2428) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field source_identity](#s-e8505dafc9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |

## Governing policies

- <a id="pa-4a3fdb65bd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-795f150398"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncCheckpoint`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23f1a9cd803ee925c19334eab9c8b6024bbbee97eb6be8c601b45924f2bf8d9e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authorization_view_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Authorization View Identity",
      "type": "string"
    },
    "catalog_cursor": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Catalog Cursor",
      "type": "string"
    },
    "format": {
      "const": "riverhog-catalog-sync/v1",
      "default": "riverhog-catalog-sync/v1",
      "title": "Format",
      "type": "string"
    },
    "source_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Identity",
      "type": "string"
    }
  },
  "required": [
    "source_identity",
    "authorization_view_identity",
    "catalog_cursor"
  ],
  "title": "CatalogSyncCheckpoint",
  "type": "object"
}
```
