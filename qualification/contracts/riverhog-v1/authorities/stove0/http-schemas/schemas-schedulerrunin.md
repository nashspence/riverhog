# schemas: SchedulerRunIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-schedulerrunin:6d5d692c93 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-904e7d48d1"></a>

- <a id="s-93c50c3642"></a>`type`: `"object"`
- <a id="s-cdf187446a"></a>`additionalProperties`: `false`
- <a id="s-89a904fd63"></a>`title`: `"SchedulerRunIn"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-010d640160"></a>`role` | no | type="string"; enum=["controller","worker","combined"]; default="combined"; title="Role" |  |
| <a id="s-083bb6e401"></a>`work_limit` | no | type="integer"; minimum=1; maximum=100; default=25; title="Work Limit" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work_limit](#s-083bb6e401) | `value · schema-value · contract_max` | shared above |

## Governing policies

- <a id="pa-6c3be82cc6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-b0f8db1c6c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerRunIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
