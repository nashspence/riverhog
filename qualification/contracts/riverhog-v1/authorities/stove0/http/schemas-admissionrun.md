# schemas: AdmissionRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionrun:c0eebc7608 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-95b5f12fb0"></a>
- <a id="s-5aaab0f6da"></a>`title`: AdmissionRun
- <a id="s-c423e97c0c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f31c640f47"></a>`failures` | no | type="array"; items=(#/components/schemas/SchedulerFailure) |  |
| <a id="s-3175a4de76"></a>`progressed` | yes | type="array"; items=(type="string") |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field failures](#s-f31c640f47) | `cardinality · items · operational_policy` | shared above |
| [field progressed](#s-3175a4de76) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: SchedulerFailure](schemas-schedulerfailure.md)

## Governing policies

- <a id="pa-3db124ba27"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-a6b21b19a7"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionRun`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0bbd87e19c427d12cb899be4bfee0310f6a65555bc5c9dd350752e31f181080e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "failures": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/SchedulerFailure"
      },
      "title": "Failures",
      "type": "array"
    },
    "progressed": {
      "items": {
        "type": "string"
      },
      "title": "Progressed",
      "type": "array"
    }
  },
  "required": [
    "progressed"
  ],
  "title": "AdmissionRun",
  "type": "object"
}
```
