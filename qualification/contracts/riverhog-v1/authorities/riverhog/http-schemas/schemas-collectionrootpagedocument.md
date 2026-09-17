# schemas: CollectionRootPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionrootpagedocument:025296e006 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-aba83af95b"></a>

- <a id="s-1c10afcf30"></a>`type`: `"object"`
- <a id="s-b99bbf8f35"></a>`additionalProperties`: `false`
- <a id="s-b5ae72aedc"></a>`required`: `["authority","start_ordinal","inputs"]`
- <a id="s-47becc1df7"></a>`title`: `"CollectionRootPageDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc3eed5ae7"></a>`authority` | yes | [ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md) |  |
| <a id="s-596a3b3f32"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)); maxItems=128; title="Inputs"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"authority-bound-start_ordinal","reason":"bounded-authority-page"} |  |
| <a id="s-31aba2331a"></a>`next_ordinal` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; title="Next Ordinal" |  |
| <a id="s-b3e9e02376"></a>`start_ordinal` | yes | type="integer"; minimum=0; title="Start Ordinal" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: maximum=128; progression={"authority":"processing-claim-inputs","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inputs](#s-596a3b3f32) | `cardinality · items · segmented_no_total_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Referenced contract dossiers

- [CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)
- [ExactSetAuthorityDocument](schemas-exactsetauthoritydocument.md)

## Governing policies

- <a id="pa-8b351d0741"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-fd9da090ba"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionRootPageDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d5e0717da2185e82b5d76eca9e34b423f9681ec772be6a07ed8f994c05614319 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "$ref": "#/components/schemas/ExactSetAuthorityDocument"
    },
    "inputs": {
      "items": {
        "$ref": "#/components/schemas/CollectionRootIdentityDocument"
      },
      "maxItems": 128,
      "title": "Inputs",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-start_ordinal",
        "reason": "bounded-authority-page"
      }
    },
    "next_ordinal": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Ordinal"
    },
    "start_ordinal": {
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "authority",
    "start_ordinal",
    "inputs"
  ],
  "title": "CollectionRootPageDocument",
  "type": "object"
}
```

</details>
