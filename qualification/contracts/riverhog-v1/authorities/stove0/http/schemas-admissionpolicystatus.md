# schemas: AdmissionPolicyStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionpolicystatus:f36d63eb97 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-6f7e6eda6d"></a>
- <a id="s-450662ebf5"></a>`title`: AdmissionPolicyStatus
- <a id="s-5d10ddd2ec"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7abab665da"></a>`authorization_view_identity` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-90608235c3"></a>`baseline_mode` | yes | type="string"; enum=["observe","backfill"] |  |
| <a id="s-9fbfaae5b4"></a>`phase` | yes | type="string"; enum=["new","baseline","following","reset_required"] |  |
| <a id="s-320c09e9e8"></a>`policy` | yes | #/components/schemas/AdmissionPolicy |  |
| <a id="s-bc8436892e"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8a42f28aef"></a>`source_identity` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-81ea7bf804"></a>`through_revision` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]*)$" |  |
| <a id="s-ded9b01a7b"></a>`updated_at` | yes | type="string"; minLength=1; maxLength=40 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-c9abb94bd5"></a>[field authorization_view_identity · string value](#s-7abab665da) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field policy_sha256](#s-bc8436892e) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-1edd94b408"></a>[field source_identity · string value](#s-8a42f28aef) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field updated_at](#s-ded9b01a7b) | `length · characters · contract_max` | maximum=40; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AdmissionPolicy](schemas-admissionpolicy.md)

## Governing policies

- <a id="pa-5eb85d0fa5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-a591928bfa"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
