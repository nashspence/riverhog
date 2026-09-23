# schemas: CollectionUploadUnitSourceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadunitsourcedocument:76e88673fe -->

One exact source range supplied in a server-planned upload unit.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e5bc88a780"></a>

- <a id="s-7edd31e1ee"></a>`type`: `"object"`
- <a id="s-4df73857fd"></a>`additionalProperties`: `false`
- <a id="s-02c5a14147"></a>`description`: `"One exact source range supplied in a server-planned upload unit."`
- <a id="s-3e239740c4"></a>`required`: `["path","offset","bytes","artifact_sha256"]`
- <a id="s-9b8774c3e9"></a>`title`: `"CollectionUploadUnitSourceDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b2c2ef931a"></a>`artifact_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Sha256" |  |
| <a id="s-f489bcd254"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-f5c3690a50"></a>`offset` | yes | type="integer"; minimum=0; title="Offset" |  |
| <a id="s-a54808ad11"></a>`path` | yes | type="string"; title="Path" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-f489bcd254) | `value · schema-value · operational_policy` | shared above |
| [field offset](#s-f5c3690a50) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_sha256](#s-b2c2ef931a) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b3fe7ae947"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d60c0ab917"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-eb4e2ef028"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitSourceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3870870a9e0b96323d9a6bb59e3124806d4f2cbfd6db047e34765004a7c96160 -->

```json
{
  "additionalProperties": false,
  "description": "One exact source range supplied in a server-planned upload unit.",
  "properties": {
    "artifact_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Artifact Sha256",
      "type": "string"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "offset": {
      "minimum": 0,
      "title": "Offset",
      "type": "integer"
    },
    "path": {
      "title": "Path",
      "type": "string"
    }
  },
  "required": [
    "path",
    "offset",
    "bytes",
    "artifact_sha256"
  ],
  "title": "CollectionUploadUnitSourceDocument",
  "type": "object"
}
```

</details>
