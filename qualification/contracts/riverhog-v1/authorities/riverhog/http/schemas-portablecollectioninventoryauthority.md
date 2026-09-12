# schemas: PortableCollectionInventoryAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-portablecollectioninventoryauthority:df9243ffe3 -->

The immutable authority shared by every bounded inventory page.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-da66940f25c6"></a>
- <a id="s-820c2dd4c674"></a>`title`: PortableCollectionInventoryAuthority
- <a id="s-3b186695d181"></a>`description`: The immutable authority shared by every bounded inventory page.
- <a id="s-77dcd58f84cb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92807b378eac"></a>`file_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-b11180e3ba18"></a>`file_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-871d5bb9aeab"></a>`header` | yes | #/components/schemas/PortableCollectionHeader |  |
| <a id="s-fb53d903c1eb"></a>`inventory_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inventory_identity](#s-fb53d903c1eb) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: PortableCollectionHeader](schemas-portablecollectionheader.md)

## Governing policies

- <a id="pa-3e96a980ba25"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-1c5f85274b01"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionInventoryAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98ff10220bdc7d51fc7ea37e5234aa18214324afa3fdcb4155f079fed05bc4cb -->

```json
{
  "additionalProperties": false,
  "description": "The immutable authority shared by every bounded inventory page.",
  "properties": {
    "file_bytes": {
      "minimum": 0,
      "title": "File Bytes",
      "type": "integer"
    },
    "file_count": {
      "minimum": 1,
      "title": "File Count",
      "type": "integer"
    },
    "header": {
      "$ref": "#/components/schemas/PortableCollectionHeader"
    },
    "inventory_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Inventory Identity",
      "type": "string"
    }
  },
  "required": [
    "header",
    "inventory_identity",
    "file_count",
    "file_bytes"
  ],
  "title": "PortableCollectionInventoryAuthority",
  "type": "object"
}
```
