# schemas: ClaimFenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-claimfencedocument:0a3144b5b0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f25a034aa4"></a>

- <a id="s-9c1790a9db"></a>`type`: `"object"`
- <a id="s-9aecb00674"></a>`additionalProperties`: `false`
- <a id="s-b7ff387d57"></a>`required`: `["id","fence"]`
- <a id="s-a43fed2df2"></a>`title`: `"ClaimFenceDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7e549c1341"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-eec0098c23"></a>`id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field id](#s-eec0098c23) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ac067e5bc9"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-95c00bea43"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ClaimFenceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b4a48bad1d6f0f80b34316c1d147a3ba35c6f8c7341ffc096d514d8b5b9b5ff -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Id",
      "type": "string"
    }
  },
  "required": [
    "id",
    "fence"
  ],
  "title": "ClaimFenceDocument",
  "type": "object"
}
```

</details>
