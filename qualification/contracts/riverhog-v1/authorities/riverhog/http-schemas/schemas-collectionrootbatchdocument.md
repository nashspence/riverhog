# schemas: CollectionRootBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionrootbatchdocument:2b49681321 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-beeeba53c5"></a>

- <a id="s-fbd771a65a"></a>`type`: `"object"`
- <a id="s-917e272ed3"></a>`additionalProperties`: `false`
- <a id="s-a9d78b9ca7"></a>`required`: `["fence","start_ordinal","inputs"]`
- <a id="s-b7d49ba41a"></a>`title`: `"CollectionRootBatchDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-94a5c12b16"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-ab04ca5dfd"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)); maxItems=128; minItems=1; title="Inputs"; uniqueItems=true; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"start_ordinal","reason":"bounded-authority-append"} |  |
| <a id="s-9c6772d7f8"></a>`start_ordinal` | yes | type="integer"; minimum=0; title="Start Ordinal" |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=1; progression={"progression":"start_ordinal"}; reason="bounded-authority-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inputs](#s-ab04ca5dfd) | `cardinality · items · segmented_no_total_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-work-authority-append/v1](../../../evidence/sources.md#e-5707b3a2d3-6a55d20aca)

## Maintained corroboration

### Referenced contract dossiers

- [CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)

## Governing policies

- <a id="pa-9568f3c73d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-b7273bfd95"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionRootBatchDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acf1b1c5363045debca142fa788e0aa4a48f38956eb2841b6c5949e94253710c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "inputs": {
      "items": {
        "$ref": "#/components/schemas/CollectionRootIdentityDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Inputs",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "start_ordinal",
        "reason": "bounded-authority-append"
      }
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
    "inputs"
  ],
  "title": "CollectionRootBatchDocument",
  "type": "object"
}
```

</details>
