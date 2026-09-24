# schemas: PendingCollectionUploadCustodyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-pendingcollectionuploadcustodyout:9b12e2b43d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-0b4513d179"></a>

- <a id="s-3fc701d0a2"></a>`type`: `"object"`
- <a id="s-10166bcc37"></a>`additionalProperties`: `false`
- <a id="s-9cd66fd9b5"></a>`required`: `["state","files","bytes"]`
- <a id="s-d780353257"></a>`title`: `"PendingCollectionUploadCustodyOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-77ea572032"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-7b7f1f216b"></a>`files` | yes | type="integer"; minimum=0; title="Files" |  |
| <a id="s-320c65c570"></a>`state` | yes | type="string"; const="pending"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-77ea572032) | `value · schema-value · operational_policy` | shared above |
| [field files](#s-7b7f1f216b) | `value · schema-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-4f7fa80cd4"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-74a9d62022"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PendingCollectionUploadCustodyOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
