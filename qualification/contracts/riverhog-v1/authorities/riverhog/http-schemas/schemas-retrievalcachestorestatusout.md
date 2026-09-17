# schemas: RetrievalCacheStoreStatusOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalcachestorestatusout:f4a4bfbb21 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-479bc23949"></a>

- <a id="s-f44ab16afe"></a>`type`: `"object"`
- <a id="s-195e0d6ba5"></a>`additionalProperties`: `false`
- <a id="s-1df12c36af"></a>`required`: `["cache_store","priority","admission_enabled","reserved_bytes","committed_bytes"]`
- <a id="s-6d9ace4fb1"></a>`title`: `"RetrievalCacheStoreStatusOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e82fb50021"></a>`admission_budget_bytes` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; title="Admission Budget Bytes" |  |
| <a id="s-b2a26342fc"></a>`admission_enabled` | yes | type="boolean"; title="Admission Enabled" |  |
| <a id="s-000e1f094b"></a>`cache_store` | yes | [RetrievalCacheStoreName](schemas-retrievalcachestorename.md) |  |
| <a id="s-603f6ec2e8"></a>`committed_bytes` | yes | type="integer"; minimum=0; title="Committed Bytes" |  |
| <a id="s-6128c90608"></a>`priority` | yes | type="integer"; minimum=1; title="Priority" |  |
| <a id="s-5f3c6ccf4d"></a>`reserved_bytes` | yes | type="integer"; minimum=0; title="Reserved Bytes" |  |

## Maintained corroboration

### Referenced contract dossiers

- [RetrievalCacheStoreName](schemas-retrievalcachestorename.md)

## Governing policies

- <a id="pa-785d0757fc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheStoreStatusOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
