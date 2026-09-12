# schemas: ControllerEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-controllerevidence:dd1235b71f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-d61f5ac1c77d"></a>
- <a id="s-59e457ab9e0d"></a>`title`: ControllerEvidence
- <a id="s-a3778ada3621"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6248d0a5d572"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f3bb08422041"></a>`execution_envelope` | yes | #/components/schemas/ExecutionEnvelope |  |
| <a id="s-3c35b188e7e8"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence_sha256](#s-6248d0a5d572) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ExecutionEnvelope](schemas-executionenvelope.md)

## Governing policies

- <a id="pa-f771ab843fc0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-69dd6637e0ef"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ControllerEvidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b115148b1c0d4c82c733fe44bf6428461157534079886b60530b9f14c008818e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "controller_evidence_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Controller Evidence Sha256",
      "type": "string"
    },
    "execution_envelope": {
      "$ref": "#/components/schemas/ExecutionEnvelope"
    },
    "format": {
      "const": "stove0-controller-evidence/v1",
      "default": "stove0-controller-evidence/v1",
      "title": "Format",
      "type": "string"
    }
  },
  "required": [
    "execution_envelope",
    "controller_evidence_sha256"
  ],
  "title": "ControllerEvidence",
  "type": "object"
}
```
