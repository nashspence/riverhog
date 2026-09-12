# schemas: SchedulerStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerstatus:7834045647 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-0950bd2aa2"></a>
- <a id="s-12a81cd590"></a>`title`: SchedulerStatus
- <a id="s-84ef31a555"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-abc73db796"></a>`interval_seconds` | yes | type="number"; additional keys=`exclusiveMinimum` |  |
| <a id="s-818b34c678"></a>`roles` | yes | type="array"; items=(type="string"; enum=["controller","worker","combined"]) |  |
| <a id="s-d854a9bbee"></a>`running` | yes | type="boolean" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field roles](#s-818b34c678) | `cardinality · items · operational_policy` | shared above |

## Governing policies

- <a id="pa-96d1052cd6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-c65c2d029a"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a4eac0cdfe53ec109a9dd097dd163c7a16e9c52f9f04684ef04035f8d303bd5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "interval_seconds": {
      "exclusiveMinimum": 0,
      "title": "Interval Seconds",
      "type": "number"
    },
    "roles": {
      "items": {
        "enum": [
          "controller",
          "worker",
          "combined"
        ],
        "type": "string"
      },
      "title": "Roles",
      "type": "array"
    },
    "running": {
      "title": "Running",
      "type": "boolean"
    }
  },
  "required": [
    "running",
    "interval_seconds",
    "roles"
  ],
  "title": "SchedulerStatus",
  "type": "object"
}
```
