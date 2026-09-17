# schemas: CollectionArtifactBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionartifactbatchdocument:4c05eb68b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5d67106b76"></a>

- <a id="s-97b4185eff"></a>`type`: `"object"`
- <a id="s-39358bc346"></a>`additionalProperties`: `false`
- <a id="s-9dab28630e"></a>`required`: `["fence","start_ordinal","artifacts"]`
- <a id="s-b4f3aea380"></a>`title`: `"CollectionArtifactBatchDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c0906aa0f"></a>`artifacts` | yes | type="array"; items=([CollectionArtifactIdentityDocument](schemas-collectionartifactidentitydocument.md)); maxItems=128; minItems=1; title="Artifacts"; uniqueItems=true; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"start_ordinal","reason":"bounded-authority-append"} |  |
| <a id="s-1c82d23b05"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-7ef1e1094b"></a>`start_ordinal` | yes | type="integer"; minimum=0; title="Start Ordinal" |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=1; progression={"progression":"start_ordinal"}; reason="bounded-authority-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-1c0906aa0f) | `cardinality · items · segmented_no_total_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-work-authority-append/v1](../../../evidence/sources.md#e-5707b3a2d3-6a55d20aca)

## Maintained corroboration

### Referenced contract dossiers

- [CollectionArtifactIdentityDocument](schemas-collectionartifactidentitydocument.md)

## Governing policies

- <a id="pa-bc2a963eda"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-692d781553"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionArtifactBatchDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 469c71d8d60e04f3a4aca348b18563579dcf35173ecb0758d55bb914324e21f3 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/CollectionArtifactIdentityDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Artifacts",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "start_ordinal",
        "reason": "bounded-authority-append"
      }
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "start_ordinal": {
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "fence",
    "start_ordinal",
    "artifacts"
  ],
  "title": "CollectionArtifactBatchDocument",
  "type": "object"
}
```

</details>
