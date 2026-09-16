# schemas: EvaluationVariant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationvariant:bd01850109 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-dcc8e57ff8"></a>

- <a id="s-21e5efb904"></a>`type`: `"object"`
- <a id="s-1cec278757"></a>`additionalProperties`: `false`
- <a id="s-c8c54dc2eb"></a>`required`: `["id"]`
- <a id="s-5828b81fc7"></a>`title`: `"EvaluationVariant"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6f8754aa9d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f77624eae7"></a>`parameters` | no | type="object"; additionalProperties=(#/components/schemas/JsonValue) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field parameters](#s-f77624eae7) | `cardinality · entries · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-a959466597"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-a0ce188e5c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationVariant`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e8958115a2db11803c3309a3413ad857ef50c32a66a2eebb3bbe835315ec19d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "parameters": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Parameters",
      "type": "object"
    }
  },
  "required": [
    "id"
  ],
  "title": "EvaluationVariant",
  "type": "object"
}
```

</details>
