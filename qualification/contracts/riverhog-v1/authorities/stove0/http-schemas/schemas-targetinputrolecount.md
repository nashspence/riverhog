# schemas: TargetInputRoleCount

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetinputrolecount:33fab925b1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-9a945539e1"></a>

- <a id="s-aab06705dd"></a>`type`: `"object"`
- <a id="s-bef6f48a5a"></a>`additionalProperties`: `false`
- <a id="s-7614ffa81f"></a>`required`: `["role","count"]`
- <a id="s-3a1c0cf219"></a>`title`: `"TargetInputRoleCount"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6dcca94a00"></a>`count` | yes | type="integer"; minimum=1; title="Count" |  |
| <a id="s-f439bc63da"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field count](#s-6dcca94a00) | `value · schema-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-c5c1b064be"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-8fcc7ae77f"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetInputRoleCount`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd93b356fb498257b255f7262952a01f1fa657555e9644325bdee9e3c4f14028 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "count": {
      "minimum": 1,
      "title": "Count",
      "type": "integer"
    },
    "role": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Role",
      "type": "string"
    }
  },
  "required": [
    "role",
    "count"
  ],
  "title": "TargetInputRoleCount",
  "type": "object"
}
```

</details>
