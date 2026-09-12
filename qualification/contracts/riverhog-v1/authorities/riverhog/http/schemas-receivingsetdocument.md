# schemas: ReceivingSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-receivingsetdocument:d32cb4d9f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-f21bf588a0"></a>
- <a id="s-dd60e3fe88"></a>`title`: ReceivingSetDocument
- <a id="s-02791136a1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9aafbb77a1"></a>`authority` | no | anyOf=#/components/schemas/ExactSetAuthorityDocument \| type="null" |  |
| <a id="s-7af5ab6592"></a>`count` | yes | type="integer"; minimum=0 |  |
| <a id="s-9d3a07ed65"></a>`state` | yes | type="string"; enum=["receiving","sealed"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field count](#s-7af5ab6592) | `value · schema-value · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)

## Governing policies

- <a id="pa-6824a91e24"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-25c2d913d0"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ReceivingSetDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bdae856bb3b833bd968c3093868c21529535df0ab22642823b261aeb4d63f282 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ExactSetAuthorityDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "count": {
      "minimum": 0,
      "title": "Count",
      "type": "integer"
    },
    "state": {
      "enum": [
        "receiving",
        "sealed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state",
    "count"
  ],
  "title": "ReceivingSetDocument",
  "type": "object"
}
```
