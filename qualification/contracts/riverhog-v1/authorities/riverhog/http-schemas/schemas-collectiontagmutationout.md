# schemas: CollectionTagMutationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiontagmutationout:2b23f499f6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b4890d412d"></a>

- <a id="s-0fc65ff4ea"></a>`type`: `"object"`
- <a id="s-5cca880c9d"></a>`additionalProperties`: `false`
- <a id="s-2226148255"></a>`required`: `["collection_id","operation_id","action","tag","changed","revision","root_sha256","tag_set_identity","head_identity","state"]`
- <a id="s-c52e9be84e"></a>`title`: `"CollectionTagMutationOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fc10489016"></a>`action` | yes | type="string"; enum=["add","remove"]; title="Action" |  |
| <a id="s-bf22d32e68"></a>`changed` | yes | type="boolean"; title="Changed" |  |
| <a id="s-25eed2eb6d"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-a43f16bafa"></a>`head_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Head Identity" |  |
| <a id="s-6553f7477d"></a>`operation_id` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"; title="Operation Id" |  |
| <a id="s-342f60d573"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991; title="Revision" |  |
| <a id="s-13adc8225c"></a>`root_sha256` | yes | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Root Sha256" |  |
| <a id="s-89bc2fdc81"></a>`state` | yes | type="string"; enum=["pending","retry_wait","succeeded"]; title="State" |  |
| <a id="s-a413ee5a7c"></a>`tag` | yes | [CollectionTag](schemas-collectiontag.md) |  |
| <a id="s-61ac4c695c"></a>`tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Tag Set Identity" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field head_identity](#s-a43f16bafa) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field revision](#s-342f60d573) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| <a id="s-b9240ca5d1"></a>[field root_sha256 · string value](#s-13adc8225c) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field tag_set_identity](#s-61ac4c695c) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [CollectionId](schemas-collectionid.md)
- [CollectionTag](schemas-collectiontag.md)

## Governing policies

- <a id="pa-f425c08c81"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-57ffd818b7"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagMutationOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
