# schemas: CollectionTagMutationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiontagmutationrequest:df7856e0e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-b4c101929169"></a>
- <a id="s-7c61c1615e39"></a>`title`: CollectionTagMutationRequest
- <a id="s-e89d2a522126"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8fd34b59f5fb"></a>`expected_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-a799f22e274e"></a>`expected_tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6dd84f6a27f9"></a>`operation_id` | yes | type="string"; minLength=1; maxLength=256; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| <a id="s-39c36b25556e"></a>`tag` | yes | #/components/schemas/CollectionTag |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field expected_revision](#s-8fd34b59f5fb) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| [field expected_tag_set_identity](#s-a799f22e274e) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field operation_id](#s-6dd84f6a27f9) | `length · characters · contract_max` | maximum=256; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionTag](schemas-collectiontag.md)

## Governing policies

- <a id="pa-609864da8652"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-d9420140fa1c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagMutationRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c1ccadd087c4e2e2878336b5365711d11b7682d9cd606b8574e8b6c4bfbe27b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "expected_revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Expected Revision",
      "type": "integer"
    },
    "expected_tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Expected Tag Set Identity",
      "type": "string"
    },
    "operation_id": {
      "maxLength": 256,
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Operation Id",
      "type": "string"
    },
    "tag": {
      "$ref": "#/components/schemas/CollectionTag"
    }
  },
  "required": [
    "operation_id",
    "tag",
    "expected_revision",
    "expected_tag_set_identity"
  ],
  "title": "CollectionTagMutationRequest",
  "type": "object"
}
```
