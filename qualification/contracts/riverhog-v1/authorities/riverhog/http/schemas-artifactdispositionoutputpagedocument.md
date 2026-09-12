# schemas: ArtifactDispositionOutputPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionoutputpagedocument:242468dcba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-ed18e124ca"></a>
- <a id="s-b4be5e79c0"></a>`title`: ArtifactDispositionOutputPageDocument
- <a id="s-d7659f4569"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38a8152364"></a>`authority` | yes | #/components/schemas/ArtifactDispositionSetIdentityDocument |  |
| <a id="s-ec50ac8374"></a>`next_ordinal` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-e1fedbe99d"></a>`outputs` | yes | type="array"; maxItems=128; items=(#/components/schemas/ArtifactDispositionOutputDocument); additional keys=`x-riverhog-extent` |  |
| <a id="s-3b4b64fa8a"></a>`start_ordinal` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: maximum=128; progression={"authority":"processing-claim-disposition-outputs","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field outputs](#s-e1fedbe99d) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionOutputDocument](schemas-artifactdispositionoutputdocument.md)
- [schemas: ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)

## Governing policies

- <a id="pa-491e7997bb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-3499f4cef5"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionOutputPageDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7fd3a513c6f8ce999a747fd64b7cf8d921fbccea66aa2295b85c0ed7540d3ac6 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "$ref": "#/components/schemas/ArtifactDispositionSetIdentityDocument"
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
    "outputs": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionOutputDocument"
      },
      "maxItems": 128,
      "title": "Outputs",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-start_ordinal",
        "reason": "bounded-authority-page"
      }
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
    "outputs"
  ],
  "title": "ArtifactDispositionOutputPageDocument",
  "type": "object"
}
```
