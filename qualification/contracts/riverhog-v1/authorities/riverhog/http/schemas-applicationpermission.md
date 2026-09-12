# schemas: ApplicationPermission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-applicationpermission:7fe84937c7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `type`: string

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ApplicationPermission`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57f5bd75d2d765c41017200a7e93d1c2d601d563e9e965b28339b25a6a2440eb -->

```json
{
  "enum": [
    "*",
    "catalog:read",
    "retrieval:manage",
    "collections:create",
    "collection-descriptions:manage",
    "collection-transforms:control",
    "collection-transforms:execute",
    "collection-tags:manage",
    "collections:delete",
    "archives:read",
    "archives:manage",
    "keys:manage",
    "quotas:manage",
    "events:read",
    "events:read_all",
    "provenance:read",
    "provenance:export"
  ],
  "type": "string"
}
```
