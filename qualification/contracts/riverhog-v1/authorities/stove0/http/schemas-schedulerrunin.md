# schemas: SchedulerRunIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerrunin:18e8ecf285 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-904e7d48d1ef"></a>
- <a id="s-89a904fd6338"></a>`title`: SchedulerRunIn
- <a id="s-93c50c3642e8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-010d64016011"></a>`role` | no | type="string"; enum=["controller","worker","combined"] |  |
| <a id="s-083bb6e401b9"></a>`work_limit` | no | type="integer"; minimum=1; maximum=100 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work_limit](#s-083bb6e401b9) | `value · schema-value · contract_max` | shared above |

## Governing policies

- <a id="pa-646942b44113"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-b1f2e74ea862"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerRunIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb6999171d296c228fdb5ff3ba85eb17a54fcbccdc9cfd7f0af37f0db3209dd5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "role": {
      "default": "combined",
      "enum": [
        "controller",
        "worker",
        "combined"
      ],
      "title": "Role",
      "type": "string"
    },
    "work_limit": {
      "default": 25,
      "maximum": 100,
      "minimum": 1,
      "title": "Work Limit",
      "type": "integer"
    }
  },
  "title": "SchedulerRunIn",
  "type": "object"
}
```
