# schemas: PendingCollectionUploadCustodyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-pendingcollectionuploadcustodyout:d5f9864287 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-0b4513d17951"></a>
- <a id="s-d78035325746"></a>`title`: PendingCollectionUploadCustodyOut
- <a id="s-3fc701d0a2f0"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-77ea5720324b"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-7b7f1f216b94"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-320c65c570ce"></a>`state` | yes | type="string"; const="pending" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-77ea5720324b) | `value · schema-value · operational_policy` | shared above |
| [field files](#s-7b7f1f216b94) | `value · schema-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-f8d5a2938680"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-72932a9ff873"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PendingCollectionUploadCustodyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bba3c06332a578b2b21e4382d6c7fffbc08107d4af02df1ec79ed7e4a839cbb7 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "files": {
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "state": {
      "const": "pending",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state",
    "files",
    "bytes"
  ],
  "title": "PendingCollectionUploadCustodyOut",
  "type": "object"
}
```
