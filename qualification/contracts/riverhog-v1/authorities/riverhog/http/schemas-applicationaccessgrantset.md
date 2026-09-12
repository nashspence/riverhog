# schemas: ApplicationAccessGrantSet

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-applicationaccessgrantset:1bd6dc5665 -->

A nonempty, duplicate-free public grant set with canonical wildcard use.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-0f785b069adf"></a>
- <a id="s-5859614d27c9"></a>`title`: ApplicationAccessGrantSet
- <a id="s-03ba28cdba56"></a>`description`: A nonempty, duplicate-free public grant set with canonical wildcard use.
- <a id="s-88edbaab8a4d"></a>`type`: array

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: ApplicationAccessGrantSet](#s-0f785b069adf) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=1; reason="wildcard-access-grant-is-exclusive"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-699746dac792"></a>allOf alternative 1 · then | `cardinality · items · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationAccessGrant](schemas-applicationaccessgrant.md)

## Governing policies

- <a id="pa-eafc4c84b852"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-21e72bebec78"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-440c4c486c10"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ApplicationAccessGrantSet`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5133bc9028edbe95e0ad3ec9b476cda4bdb9c92a1c4942973660db9d1df5bd06 -->

```json
{
  "allOf": [
    {
      "if": {
        "contains": {
          "properties": {
            "permission": {
              "const": "*"
            }
          },
          "required": [
            "permission"
          ],
          "type": "object"
        }
      },
      "then": {
        "maxItems": 1,
        "x-riverhog-extent": {
          "policy": "contract_max",
          "reason": "wildcard-access-grant-is-exclusive"
        }
      }
    }
  ],
  "description": "A nonempty, duplicate-free public grant set with canonical wildcard use.",
  "items": {
    "$ref": "#/components/schemas/ApplicationAccessGrant"
  },
  "minItems": 1,
  "title": "ApplicationAccessGrantSet",
  "type": "array",
  "uniqueItems": true
}
```
