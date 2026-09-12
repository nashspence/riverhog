# schemas: TargetProductionAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetproductionauthority:5d406c80aa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-c3ee643298ca"></a>
- <a id="s-033fae8d9ae6"></a>`title`: TargetProductionAuthority
- <a id="s-6d6f0f906acd"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35a1adfc05ff"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-74bb4956e038"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ad8b45f4a08a"></a>`format` | no | type="string"; const="stove0-target-production/v1" |  |
| <a id="s-47b3af76c1fa"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4c5e8f7dd184"></a>`outputs` | yes | #/components/schemas/OutputArtifactSetIdentity |  |
| <a id="s-261797bf1727"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ac236757260d"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3899c18b1755"></a>`riverhog_disposition_set` | yes | #/components/schemas/ArtifactDispositionSetIdentity |  |
| <a id="s-196ffe30d1de"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-488a513f4e6b"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field disposition_sha256](#s-74bb4956e038) | `length · characters · fixed` | shared above |
| [field job_id](#s-47b3af76c1fa) | `length · characters · fixed` | shared above |
| [field plan_sha256](#s-261797bf1727) | `length · characters · fixed` | shared above |
| [field production_sha256](#s-ac236757260d) | `length · characters · fixed` | shared above |
| [field source_edge_sha256](#s-488a513f4e6b) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionSetIdentity](schemas-artifactdispositionsetidentity.md)
- [schemas: OutputArtifactSetIdentity](schemas-outputartifactsetidentity.md)

## Governing policies

- <a id="pa-302891756e3b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-8c43dfe3af27"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetProductionAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f52dbbdc45a3bad1264269aeec9fc665a9b79c6a478c0af16d3ccd200f588c8b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "disposition_count": {
      "minimum": 1,
      "title": "Disposition Count",
      "type": "integer"
    },
    "disposition_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Disposition Sha256",
      "type": "string"
    },
    "format": {
      "const": "stove0-target-production/v1",
      "default": "stove0-target-production/v1",
      "title": "Format",
      "type": "string"
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "outputs": {
      "$ref": "#/components/schemas/OutputArtifactSetIdentity"
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "production_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Production Sha256",
      "type": "string"
    },
    "riverhog_disposition_set": {
      "$ref": "#/components/schemas/ArtifactDispositionSetIdentity"
    },
    "source_edge_count": {
      "minimum": 1,
      "title": "Source Edge Count",
      "type": "integer"
    },
    "source_edge_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Edge Sha256",
      "type": "string"
    }
  },
  "required": [
    "job_id",
    "plan_sha256",
    "outputs",
    "disposition_count",
    "disposition_sha256",
    "source_edge_count",
    "source_edge_sha256",
    "riverhog_disposition_set",
    "production_sha256"
  ],
  "title": "TargetProductionAuthority",
  "type": "object"
}
```
