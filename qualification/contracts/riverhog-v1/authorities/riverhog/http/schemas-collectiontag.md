# schemas: CollectionTag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiontag:c335ce2d1c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `type`: string

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| encoded-size | bytes | `contract_max` | maximum=65536, reason=bounded-human-authored-collection-tag |
| length | characters | `contract_max` | maximum=65536, minimum=1, reason=schema-maximum |

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

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTag`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b6917e633cfcae1991f58fecc90c7c91406e442bcbfea53917a973300b49dc7 -->

```json
{
  "maxLength": 65536,
  "minLength": 1,
  "type": "string",
  "x-riverhog-encoded-bytes-max": 65536,
  "x-riverhog-extent": {
    "policy": "contract_max",
    "reason": "bounded-human-authored-collection-tag"
  },
  "x-unicode-normalization": "NFC"
}
```
