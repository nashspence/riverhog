# schemas: CollectionArtifactPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionartifactpagedocument:67e047d193 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-adaba01f6758"></a>
- <a id="s-2ac9b854b4ca"></a>`title`: CollectionArtifactPageDocument
- <a id="s-3e75f6b0b5f4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dec7134215b9"></a>`artifacts` | yes | type="array"; maxItems=128; items=(#/components/schemas/CollectionArtifactIdentityDocument); additional keys=`x-riverhog-extent` |  |
| <a id="s-75ddf70b2de6"></a>`authority` | yes | #/components/schemas/ArtifactSetAuthorityDocument |  |
| <a id="s-5bea015a1e4c"></a>`next_ordinal` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-cc8bfd36cbd8"></a>`start_ordinal` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)

Shared facts for every subject below: maximum=128; progression={"authority":"processing-claim-artifacts","authority_parameter":"authority_sha256","cursor_parameter":"start_ordinal","fixed_limit":128,"kind":"exact-authority-page"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-dec7134215b9) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSetAuthorityDocument](schemas-artifactsetauthoritydocument.md)
- [schemas: CollectionArtifactIdentityDocument](schemas-collectionartifactidentitydocument.md)

## Governing policies

- <a id="pa-eebb524c865d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-59797cb6f965"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionArtifactPageDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d36dba622d1c18ba09c32f815f78c7a8f4a1995796134145366ce2ddaee0d3d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/CollectionArtifactIdentityDocument"
      },
      "maxItems": 128,
      "title": "Artifacts",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-start_ordinal",
        "reason": "bounded-authority-page"
      }
    },
    "authority": {
      "$ref": "#/components/schemas/ArtifactSetAuthorityDocument"
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
    "artifacts"
  ],
  "title": "CollectionArtifactPageDocument",
  "type": "object"
}
```
