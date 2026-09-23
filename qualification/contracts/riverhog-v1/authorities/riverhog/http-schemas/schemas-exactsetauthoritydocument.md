# schemas: ExactSetAuthorityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-exactsetauthoritydocument:5edf4844c2 -->

Small immutable identity for an exact canonically ordered logical set.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-fe161ba236"></a>

- <a id="s-7e69e5db62"></a>`type`: `"object"`
- <a id="s-446126c9d1"></a>`additionalProperties`: `false`
- <a id="s-0df13433b0"></a>`description`: `"Small immutable identity for an exact canonically ordered logical set."`
- <a id="s-b977b9a19e"></a>`required`: `["count","sha256"]`
- <a id="s-044c8a5ab0"></a>`title`: `"ExactSetAuthorityDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-edd753f1d8"></a>`count` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-9afef3e97a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-9afef3e97a) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-fd73b42066"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-aef9b3f177"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ExactSetAuthorityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 210b90e874e1008e8664061ef9d5740a12f4f1df08272146924bbc05bb3e8642 -->

```json
{
  "additionalProperties": false,
  "description": "Small immutable identity for an exact canonically ordered logical set.",
  "properties": {
    "count": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "count",
    "sha256"
  ],
  "title": "ExactSetAuthorityDocument",
  "type": "object"
}
```

</details>
