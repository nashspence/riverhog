# schemas: ImmutableFileIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-immutablefileidentitydocument:753c8854b1 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ImmutableFileIdentityDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CanonicalRelPath](schemas-canonicalrelpath.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ImmutableFileIdentityDocument
- `description`: The exact path, length, and plaintext digest shared by file projections.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `bytes` | yes | integer |  |
| `path` | yes | #/components/schemas/CanonicalRelPath |  |
| `sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c2805986926e29a6c406c80c224e37296c6bd56550bcb6c78776369f757a75c -->

```json
{
  "additionalProperties": false,
  "description": "The exact path, length, and plaintext digest shared by file projections.",
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "path",
    "bytes",
    "sha256"
  ],
  "title": "ImmutableFileIdentityDocument",
  "type": "object"
}
```
