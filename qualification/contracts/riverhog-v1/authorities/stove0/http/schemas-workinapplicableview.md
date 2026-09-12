# schemas: WorkInapplicableView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workinapplicableview:ee70b9aed7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-8f187600fb"></a>
- <a id="s-07ac019401"></a>`title`: WorkInapplicableView
- <a id="s-ea0d0c511b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a3ff8d188d"></a>`code` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-3e3aa8cd68"></a>`message` | yes | type="string"; minLength=1; maxLength=1000 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field code](#s-a3ff8d188d) | `length · characters · contract_max` | maximum=160 |
| [field message](#s-3e3aa8cd68) | `length · characters · contract_max` | maximum=1000 |

## Governing policies

- <a id="pa-f3d989b33c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-832fccee47"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkInapplicableView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6656e1447c29d937de4ad76ebaf92f3aee0431f6a1303726a004083da0376789 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Code",
      "type": "string"
    },
    "message": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Message",
      "type": "string"
    }
  },
  "required": [
    "code",
    "message"
  ],
  "title": "WorkInapplicableView",
  "type": "object"
}
```
