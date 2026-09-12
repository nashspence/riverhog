# schemas: ProcessingClaimOutcomeSettlementDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimoutcomesettlementdocument:fb9ea922ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b7a81be779"></a>
- <a id="s-86326ca924"></a>`title`: ProcessingClaimOutcomeSettlementDocument
- <a id="s-4aff1b04cd"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66e7037bf1"></a>`outcomes` | yes | #/components/schemas/ExactSetAuthorityDocument |  |
| <a id="s-c9a1869ba6"></a>`retirement_grace_seconds` | yes | type="integer"; minimum=0 |  |
| <a id="s-ca68cd4ae2"></a>`retirement_policy` | yes | type="string"; enum=["retain","retire-after-verified-output"] |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)

## Governing policies

- <a id="pa-e22884f722"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimOutcomeSettlementDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80797ec5debdb6aa1dc3e41984cbbe33be59aad57827c3211c7c54b55be29e1e -->

```json
{
  "additionalProperties": false,
  "if": {
    "properties": {
      "retirement_policy": {
        "const": "retain"
      }
    }
  },
  "properties": {
    "outcomes": {
      "$ref": "#/components/schemas/ExactSetAuthorityDocument"
    },
    "retirement_grace_seconds": {
      "minimum": 0,
      "title": "Retirement Grace Seconds",
      "type": "integer"
    },
    "retirement_policy": {
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Retirement Policy",
      "type": "string"
    }
  },
  "required": [
    "outcomes",
    "retirement_policy",
    "retirement_grace_seconds"
  ],
  "then": {
    "properties": {
      "retirement_grace_seconds": {
        "const": 0
      }
    }
  },
  "title": "ProcessingClaimOutcomeSettlementDocument",
  "type": "object"
}
```
