# schemas: WorkUpdatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workupdatedeventdata:52e558b11f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4aa899f5f5"></a>

- <a id="s-12839df953"></a>`type`: `"object"`
- <a id="s-a573a72290"></a>`additionalProperties`: `false`
- <a id="s-7645d51bd5"></a>`required`: `["work_id","phase","revision"]`
- <a id="s-43d601ed31"></a>`title`: `"WorkUpdatedEventData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5598b9d738"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"]; title="Phase" |  |
| <a id="s-4123ed42c3"></a>`revision` | yes | type="integer"; minimum=2; title="Revision" |  |
| <a id="s-12842d72c4"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work_id](#s-12842d72c4) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-a301dc0af4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-4c927e0a26"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkUpdatedEventData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ccdca8ddb59e6f6e51efd841d9b827e5b954d845a51a4146d07e9d1b2d80900a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "phase": {
      "enum": [
        "eligible",
        "claimed",
        "observing",
        "planning",
        "target_preflight",
        "queued",
        "executing",
        "output_finalizing",
        "verifying",
        "settled",
        "retirement_pending",
        "coordinating",
        "abandon_pending",
        "complete",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "Phase",
      "type": "string"
    },
    "revision": {
      "minimum": 2,
      "title": "Revision",
      "type": "integer"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "work_id",
    "phase",
    "revision"
  ],
  "title": "WorkUpdatedEventData",
  "type": "object"
}
```

</details>
