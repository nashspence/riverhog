# schemas: SearchFileOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-searchfileout:5e6231963f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-31df2cc3b8"></a>

- <a id="s-4850f1dd28"></a>`type`: `"object"`
- <a id="s-c305b26e13"></a>`additionalProperties`: `false`
- <a id="s-f02029bfc3"></a>`required`: `["path","bytes","sha256","file_ref","collection_id"]`
- <a id="s-2bfc429da3"></a>`title`: `"SearchFileOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cb79bb77a7"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-71e9d43dba"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-64e1b4cfdb"></a>`file_ref` | yes | type="string"; title="File Ref" |  |
| <a id="s-f85fe65020"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-19884818f4"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-cb79bb77a7) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-19884818f4) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-3fbb8d040d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-a531e14a1c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-bc30f5420e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/SearchFileOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8dbb3defa61ae262a611e01f03a2c21b15a9e4720ace7b1aef5f6ec173fae4c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "file_ref": {
      "title": "File Ref",
      "type": "string"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "path",
    "bytes",
    "sha256",
    "file_ref",
    "collection_id"
  ],
  "title": "SearchFileOut",
  "type": "object"
}
```

</details>
