# schemas: ObserverImplementation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observerimplementation:a7cff22e58 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObserverImplementation`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=120, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: ObserverImplementation
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `descriptor_sha256` | yes | string |  |
| `id` | yes | string |  |
| `protocol` | no | string |  |
| `source_revision` | yes | string |  |
| `version` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ffdaa644c4d7b3d7f3e17d66ebec04a5997be6e6e59c8839ae0685b1250ba75 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Descriptor Sha256",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "protocol": {
      "const": "stove0-content-observer/v1",
      "default": "stove0-content-observer/v1",
      "title": "Protocol",
      "type": "string"
    },
    "source_revision": {
      "maxLength": 200,
      "minLength": 1,
      "title": "Source Revision",
      "type": "string"
    },
    "version": {
      "maxLength": 120,
      "minLength": 1,
      "title": "Version",
      "type": "string"
    }
  },
  "required": [
    "id",
    "version",
    "source_revision",
    "descriptor_sha256"
  ],
  "title": "ObserverImplementation",
  "type": "object"
}
```
