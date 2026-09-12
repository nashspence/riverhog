# schemas: CollectionDeletionResultOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondeletionresultout:abf4078ec6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-b0cf1465ab"></a>
- <a id="s-8ae75e8380"></a>`title`: CollectionDeletionResultOut
- <a id="s-0b0225496e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3b542ea2db"></a>`bytes` | yes | type="integer" |  |
| <a id="s-e8df16b2b3"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-348721ef1f"></a>`files` | yes | type="integer" |  |
| <a id="s-f27c30182f"></a>`remote_storage_bytes` | yes | type="integer" |  |
| <a id="s-534c1b5c15"></a>`status` | yes | type="string"; enum=["deleting","deleted","already_absent"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-3b542ea2db) | `value · schema-value · operational_policy` | shared above |
| [field files](#s-348721ef1f) | `value · schema-value · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-fe1b9d13f1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-44a6b449ba"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletionResultOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c29c7974ad1cb9ee7c6d98e1114fd78f7388b444c98a4d910d5a816982a83be -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "files": {
      "title": "Files",
      "type": "integer"
    },
    "remote_storage_bytes": {
      "title": "Remote Storage Bytes",
      "type": "integer"
    },
    "status": {
      "enum": [
        "deleting",
        "deleted",
        "already_absent"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "status",
    "collection_id",
    "files",
    "bytes",
    "remote_storage_bytes"
  ],
  "title": "CollectionDeletionResultOut",
  "type": "object"
}
```
