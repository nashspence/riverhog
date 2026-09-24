# schemas: AdmissionRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-admissionrun:e0b948a677 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-95b5f12fb0"></a>

- <a id="s-c423e97c0c"></a>`type`: `"object"`
- <a id="s-da45f32181"></a>`additionalProperties`: `false`
- <a id="s-53bb43e936"></a>`required`: `["progressed"]`
- <a id="s-5aaab0f6da"></a>`title`: `"AdmissionRun"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f31c640f47"></a>`failures` | no | type="array"; default=[]; items=([SchedulerFailure](schemas-schedulerfailure.md)); title="Failures" |  |
| <a id="s-3175a4de76"></a>`progressed` | yes | type="array"; items=(type="string"); title="Progressed" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field failures](#s-f31c640f47) | `cardinality · items · operational_policy` | shared above |
| [field progressed](#s-3175a4de76) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract elements

- [SchedulerFailure](schemas-schedulerfailure.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dc84e452ab"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-99866cbe67"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionRun`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
