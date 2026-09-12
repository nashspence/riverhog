# schemas: RetrievalCacheStoreStatusOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcachestorestatusout:593a25b981 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-479bc23949b8"></a>
- <a id="s-6d9ace4fb177"></a>`title`: RetrievalCacheStoreStatusOut
- <a id="s-f44ab16afe94"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e82fb5002110"></a>`admission_budget_bytes` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-b2a26342fcef"></a>`admission_enabled` | yes | type="boolean" |  |
| <a id="s-000e1f094b5f"></a>`cache_store` | yes | #/components/schemas/RetrievalCacheStoreName |  |
| <a id="s-603f6ec2e84f"></a>`committed_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-6128c906080d"></a>`priority` | yes | type="integer"; minimum=1 |  |
| <a id="s-5f3c6ccf4d06"></a>`reserved_bytes` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalCacheStoreName](schemas-retrievalcachestorename.md)

## Governing policies

- <a id="pa-78a94d181541"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheStoreStatusOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 47a05eaf9ad5742e01f81bcc666f488950151a064fc3351783353cb032473daf -->

```json
{
  "additionalProperties": false,
  "properties": {
    "admission_budget_bytes": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Admission Budget Bytes"
    },
    "admission_enabled": {
      "title": "Admission Enabled",
      "type": "boolean"
    },
    "cache_store": {
      "$ref": "#/components/schemas/RetrievalCacheStoreName"
    },
    "committed_bytes": {
      "minimum": 0,
      "title": "Committed Bytes",
      "type": "integer"
    },
    "priority": {
      "minimum": 1,
      "title": "Priority",
      "type": "integer"
    },
    "reserved_bytes": {
      "minimum": 0,
      "title": "Reserved Bytes",
      "type": "integer"
    }
  },
  "required": [
    "cache_store",
    "priority",
    "admission_enabled",
    "reserved_bytes",
    "committed_bytes"
  ],
  "title": "RetrievalCacheStoreStatusOut",
  "type": "object"
}
```
