# schemas: ClaimFenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-claimfencedocument:61c366d57b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-f25a034aa491"></a>
- <a id="s-a43fed2df2f3"></a>`title`: ClaimFenceDocument
- <a id="s-9c1790a9dbcb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7e549c1341e8"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-eec0098c2309"></a>`id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field id](#s-eec0098c2309) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-3703ee481fb1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-99f9ab116a32"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ClaimFenceDocument`

### Exact owned JSON

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
