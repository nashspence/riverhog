# schemas: CollectionTagMutationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiontagmutationout:6300d19241 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-b4890d412d9a"></a>
- <a id="s-c52e9be84ec6"></a>`title`: CollectionTagMutationOut
- <a id="s-0fc65ff4ea0d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fc10489016a6"></a>`action` | yes | type="string"; enum=["add","remove"] |  |
| <a id="s-bf22d32e68ec"></a>`changed` | yes | type="boolean" |  |
| <a id="s-25eed2eb6dee"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-a43f16bafabf"></a>`head_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6553f7477d50"></a>`operation_id` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| <a id="s-342f60d57313"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-13adc8225c63"></a>`root_sha256` | yes | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-89bc2fdc8109"></a>`state` | yes | type="string"; enum=["pending","retry_wait","succeeded"] |  |
| <a id="s-a413ee5a7cc1"></a>`tag` | yes | #/components/schemas/CollectionTag |  |
| <a id="s-61ac4c695c47"></a>`tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field head_identity](#s-a43f16bafabf) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field revision](#s-342f60d57313) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| <a id="s-b9240ca5d155"></a>field root_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field tag_set_identity](#s-61ac4c695c47) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionTag](schemas-collectiontag.md)

## Governing policies

- <a id="pa-307196625fec"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-8d2bd069f1fb"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagMutationOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b998f00d5a88ea2831690b43733ac89b267e4a3befa8f624f559bcf642a0509 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "action": {
      "enum": [
        "add",
        "remove"
      ],
      "title": "Action",
      "type": "string"
    },
    "changed": {
      "title": "Changed",
      "type": "boolean"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "head_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Head Identity",
      "type": "string"
    },
    "operation_id": {
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Operation Id",
      "type": "string"
    },
    "revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "root_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Root Sha256"
    },
    "state": {
      "enum": [
        "pending",
        "retry_wait",
        "succeeded"
      ],
      "title": "State",
      "type": "string"
    },
    "tag": {
      "$ref": "#/components/schemas/CollectionTag"
    },
    "tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Tag Set Identity",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "operation_id",
    "action",
    "tag",
    "changed",
    "revision",
    "root_sha256",
    "tag_set_identity",
    "head_identity",
    "state"
  ],
  "title": "CollectionTagMutationOut",
  "type": "object"
}
```
