# schemas: SchedulerRunRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-schedulerrunrequest:eeba02822d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-c0d9e57b7b"></a>

- <a id="s-c36c474bd9"></a>`type`: `"object"`
- <a id="s-336fd5fe69"></a>`additionalProperties`: `false`
- <a id="s-970c4dc59a"></a>`title`: `"SchedulerRunRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-90725ed26a"></a>`role` | no | type="string"; enum=["controller","worker","combined"]; default="combined"; title="Role" |  |
| <a id="s-e0ab285fcc"></a>`work_limit` | no | type="integer"; minimum=1; maximum=100; default=25; title="Work Limit" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work_limit](#s-e0ab285fcc) | `value · schema-value · contract_max` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1a1bb6a281"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-f984232e7d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerRunRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8788c0aab24e4e2a8cc8507af7882a3e0f0edcec3c15ba8cb7b93d2f400dba4c -->

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
  "title": "SchedulerRunRequest",
  "type": "object"
}
```

</details>
