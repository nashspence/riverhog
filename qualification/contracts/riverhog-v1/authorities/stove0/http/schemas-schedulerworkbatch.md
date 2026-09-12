# schemas: SchedulerWorkBatch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerworkbatch:0c29b4c561 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-91b2b76dcb78"></a>
- <a id="s-722e47111c1e"></a>`title`: SchedulerWorkBatch
- <a id="s-264d2157f8b8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b980481048e8"></a>`cursor` | yes | type="string" |  |
| <a id="s-d6d47a1ee905"></a>`failures` | yes | type="array"; items=(#/components/schemas/SchedulerFailure) |  |
| <a id="s-09d7eabe63a8"></a>`next_cursor` | yes | type="string" |  |
| <a id="s-66986d978d4b"></a>`progressed` | yes | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-257ee67e97d3"></a>`role` | yes | type="string"; enum=["controller","worker","combined"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field failures](#s-d6d47a1ee905) | `cardinality · items · operational_policy` | shared above |
| [field progressed](#s-66986d978d4b) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-8a9208b69af3"></a>field progressed · items | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: SchedulerFailure](schemas-schedulerfailure.md)

## Governing policies

- <a id="pa-26b6485469d3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-4a3bfc2f95af"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-580f778668de"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerWorkBatch`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8bc39e7070ed70678d9f49add76a6806022e801c3461554beebb6ae95228e285 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "cursor": {
      "title": "Cursor",
      "type": "string"
    },
    "failures": {
      "items": {
        "$ref": "#/components/schemas/SchedulerFailure"
      },
      "title": "Failures",
      "type": "array"
    },
    "next_cursor": {
      "title": "Next Cursor",
      "type": "string"
    },
    "progressed": {
      "items": {
        "pattern": "^[0-9a-f]{64}$",
        "type": "string"
      },
      "title": "Progressed",
      "type": "array"
    },
    "role": {
      "enum": [
        "controller",
        "worker",
        "combined"
      ],
      "title": "Role",
      "type": "string"
    }
  },
  "required": [
    "role",
    "cursor",
    "next_cursor",
    "progressed",
    "failures"
  ],
  "title": "SchedulerWorkBatch",
  "type": "object"
}
```
