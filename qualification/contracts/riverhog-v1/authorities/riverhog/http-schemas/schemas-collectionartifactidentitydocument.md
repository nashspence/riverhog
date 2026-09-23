# schemas: CollectionArtifactIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionartifactidentitydocument:90c16600da -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-527d6f8a60"></a>

- <a id="s-d960036525"></a>`type`: `"object"`
- <a id="s-8748001f64"></a>`additionalProperties`: `false`
- <a id="s-60629dc1e5"></a>`required`: `["collection","path","bytes","sha256"]`
- <a id="s-b2bf06d31f"></a>`title`: `"CollectionArtifactIdentityDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e1311e93b0"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-4343b483d2"></a>`collection` | yes | [CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md) |  |
| <a id="s-147f0e359d"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-bd06c21705"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-e1311e93b0) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-bd06c21705) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0d74c858c1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d67c11c35e"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-f561f7c3e4"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionArtifactIdentityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b8626cf1976bfbc2e82542d7692f060d2d1c3405916812bac4a427a99f649c0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection": {
      "$ref": "#/components/schemas/CollectionRootIdentityDocument"
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
    "collection",
    "path",
    "bytes",
    "sha256"
  ],
  "title": "CollectionArtifactIdentityDocument",
  "type": "object"
}
```

</details>
