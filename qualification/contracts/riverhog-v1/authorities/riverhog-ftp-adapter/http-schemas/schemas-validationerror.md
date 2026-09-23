# schemas: ValidationError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog-ftp-adapter:schemas-validationerror:ee6dc1422e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-955a59da86"></a>

- <a id="s-6480a4ffea"></a>`type`: `"object"`
- <a id="s-31d38f45de"></a>`required`: `["loc","msg","type"]`
- <a id="s-efd9bb0899"></a>`title`: `"ValidationError"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ceb436b588"></a>`ctx` | no | type="object"; title="Context" |  |
| <a id="s-6e2e810210"></a>`input` | no | title="Input" |  |
| <a id="s-5d84247a94"></a>`loc` | yes | type="array"; items=(anyOf=[(type="string"); (type="integer")]); title="Location" |  |
| <a id="s-e9a26048d6"></a>`msg` | yes | type="string"; title="Message" |  |
| <a id="s-be50ac7c9d"></a>`type` | yes | type="string"; title="Error Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field loc](#s-5d84247a94) | `cardinality · items · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-fd134c659f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-809f75b3f4"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog-ftp-adapter](../../../evidence/sources/authorities.md#src-c3a51ac29a) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/components/schemas/ValidationError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca285d0c0e64c42844efd2e860edb239e77cc49d87f065c5ccb7d2f55331b862 -->

```json
{
  "properties": {
    "ctx": {
      "title": "Context",
      "type": "object"
    },
    "input": {
      "title": "Input"
    },
    "loc": {
      "items": {
        "anyOf": [
          {
            "type": "string"
          },
          {
            "type": "integer"
          }
        ]
      },
      "title": "Location",
      "type": "array"
    },
    "msg": {
      "title": "Message",
      "type": "string"
    },
    "type": {
      "title": "Error Type",
      "type": "string"
    }
  },
  "required": [
    "loc",
    "msg",
    "type"
  ],
  "title": "ValidationError",
  "type": "object"
}
```

</details>
