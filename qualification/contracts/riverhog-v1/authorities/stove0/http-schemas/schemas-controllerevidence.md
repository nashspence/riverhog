# schemas: ControllerEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-controllerevidence:8d2c5b82f8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d61f5ac1c7"></a>

- <a id="s-a3778ada36"></a>`type`: `"object"`
- <a id="s-3fd02fda35"></a>`additionalProperties`: `false`
- <a id="s-d7968e480b"></a>`required`: `["execution_envelope","controller_evidence_sha256"]`
- <a id="s-59e457ab9e"></a>`title`: `"ControllerEvidence"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6248d0a5d5"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Controller Evidence Sha256" |  |
| <a id="s-f3bb084220"></a>`execution_envelope` | yes | [ExecutionEnvelope](schemas-executionenvelope.md) |  |
| <a id="s-3c35b188e7"></a>`format` | no | type="string"; const="stove0-controller-evidence/v1"; default="stove0-controller-evidence/v1"; title="Format" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence_sha256](#s-6248d0a5d5) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ExecutionEnvelope](schemas-executionenvelope.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c96c0d0065"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d935bc2c33"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ControllerEvidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
