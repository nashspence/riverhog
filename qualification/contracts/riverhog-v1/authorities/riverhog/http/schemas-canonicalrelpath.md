# schemas: CanonicalRelPath

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-canonicalrelpath:ba7377c5c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `type`: string
- `format`: riverhog-canonical-relpath-v1

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CanonicalRelPath`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa3392977e23c3c604f49fed3024ddfc514d329289c0f292c3ebe3e4c3748e66 -->

```json
{
  "allOf": [
    {
      "not": {
        "pattern": "(?:^|/)\\.{1,2}(?:/|$)"
      }
    },
    {
      "not": {
        "pattern": "^\\s|\\s$"
      }
    }
  ],
  "format": "riverhog-canonical-relpath-v1",
  "maxLength": 4096,
  "minLength": 1,
  "pattern": "^[^/\\\\]+(?:/[^/\\\\]+)*$",
  "type": "string",
  "x-unicode-normalization": "NFC"
}
```
