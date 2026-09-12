# schemas: AdmissionPolicyStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionpolicystatus:f36d63eb97 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- `title`: AdmissionPolicyStatus
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authorization_view_identity` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| `baseline_mode` | yes | type="string"; enum=["observe","backfill"] |  |
| `phase` | yes | type="string"; enum=["new","baseline","following","reset_required"] |  |
| `policy` | yes | #/components/schemas/AdmissionPolicy |  |
| `policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `source_identity` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| `through_revision` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]*)$" |  |
| `updated_at` | yes | type="string"; minLength=1; maxLength=40 |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=40, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AdmissionPolicy](schemas-admissionpolicy.md)

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

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionPolicyStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7404ffc49e24c3273ed3c6d1d48218bb2dc3b14579283217e176b3ecc4b7983a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authorization_view_identity": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Authorization View Identity"
    },
    "baseline_mode": {
      "enum": [
        "observe",
        "backfill"
      ],
      "title": "Baseline Mode",
      "type": "string"
    },
    "phase": {
      "enum": [
        "new",
        "baseline",
        "following",
        "reset_required"
      ],
      "title": "Phase",
      "type": "string"
    },
    "policy": {
      "$ref": "#/components/schemas/AdmissionPolicy"
    },
    "policy_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Policy Sha256",
      "type": "string"
    },
    "source_identity": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Source Identity"
    },
    "through_revision": {
      "pattern": "^(?:0|[1-9][0-9]*)$",
      "title": "Through Revision",
      "type": "string"
    },
    "updated_at": {
      "maxLength": 40,
      "minLength": 1,
      "title": "Updated At",
      "type": "string"
    }
  },
  "required": [
    "policy",
    "policy_sha256",
    "phase",
    "baseline_mode",
    "through_revision",
    "updated_at"
  ],
  "title": "AdmissionPolicyStatus",
  "type": "object"
}
```
