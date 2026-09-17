# schemas: ProcessingClaimFiltersDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimfiltersdocument:3b7fc639b7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-a70300dbcf"></a>

- <a id="s-0fb9297f4f"></a>`type`: `"object"`
- <a id="s-a14ccb7c00"></a>`additionalProperties`: `false`
- <a id="s-fd946a5579"></a>`title`: `"ProcessingClaimFiltersDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6a7a54d76b"></a>`state` | no | anyOf=[(type="string"; enum=["active","settled","retiring","abandoned","released"]); (type="null")]; title="State" |  |

## Governing policies

- <a id="pa-07621a1aad"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimFiltersDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59b4d8bb229a8c43d763c982eebd067a71e6bccd1c8851ee169948435abc1158 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "state": {
      "anyOf": [
        {
          "enum": [
            "active",
            "settled",
            "retiring",
            "abandoned",
            "released"
          ],
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "State"
    }
  },
  "title": "ProcessingClaimFiltersDocument",
  "type": "object"
}
```

</details>
