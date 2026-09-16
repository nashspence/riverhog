# schemas: ErrorBody

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog-ftp-adapter:schemas-errorbody:09dd5efaf5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-18b7660b76"></a>

- <a id="s-6866a946b7"></a>`type`: `"object"`
- <a id="s-42dae03fb7"></a>`additionalProperties`: `false`
- <a id="s-7d37cc2cb5"></a>`required`: `["code","message"]`
- <a id="s-5b1d2cf515"></a>`title`: `"ErrorBody"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cd03e78d13"></a>`code` | yes | type="string"; minLength=1 |  |
| <a id="s-d62f0c60eb"></a>`details` | no | anyOf=(type="object"; additionalProperties=true) \| (type="null"); default=null |  |
| <a id="s-47d4490529"></a>`message` | yes | type="string"; minLength=1 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-a569b40d2f"></a>[field details · object value](#s-d62f0c60eb) | `cardinality · entries · operational_policy` | shared above |

## Governing policies

- <a id="pa-a64ae9d696"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-3cbc84abc5"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/components/schemas/ErrorBody`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6760c34bd592b6639dc349282b6e02f6cebfa1985aa1a5b2c1943c0faa367741 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "minLength": 1,
      "title": "Code",
      "type": "string"
    },
    "details": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Details"
    },
    "message": {
      "minLength": 1,
      "title": "Message",
      "type": "string"
    }
  },
  "required": [
    "code",
    "message"
  ],
  "title": "ErrorBody",
  "type": "object"
}
```

</details>
