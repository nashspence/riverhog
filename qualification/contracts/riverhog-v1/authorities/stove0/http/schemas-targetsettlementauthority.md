# schemas: TargetSettlementAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetsettlementauthority:1928061221 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: TargetSettlementAuthority
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `format` | no | type="string"; const="stove0-target-settlement/v1" |  |
| `job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `output_bindings` | yes | #/components/schemas/TargetOutputBindingSetIdentity |  |
| `output_collection` | yes | #/components/schemas/OutputCollectionRef |  |
| `production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: OutputCollectionRef](schemas-outputcollectionref.md)
- [schemas: TargetOutputBindingSetIdentity](schemas-targetoutputbindingsetidentity.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetSettlementAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a811b1b88f435f8192083bd5bb9cab621322a012a9d17a39beebcbf46f0b2e02 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-target-settlement/v1",
      "default": "stove0-target-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "output_bindings": {
      "$ref": "#/components/schemas/TargetOutputBindingSetIdentity"
    },
    "output_collection": {
      "$ref": "#/components/schemas/OutputCollectionRef"
    },
    "production_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Production Sha256",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    }
  },
  "required": [
    "job_id",
    "production_sha256",
    "output_collection",
    "output_bindings",
    "settlement_sha256"
  ],
  "title": "TargetSettlementAuthority",
  "type": "object"
}
```
