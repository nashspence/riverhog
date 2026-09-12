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

<a id="s-c3ee643298"></a>
- <a id="s-033fae8d9a"></a>`title`: TargetProductionAuthority
- <a id="s-6d6f0f906a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35a1adfc05"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-74bb4956e0"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ad8b45f4a0"></a>`format` | no | type="string"; const="stove0-target-production/v1" |  |
| <a id="s-47b3af76c1"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4c5e8f7dd1"></a>`outputs` | yes | #/components/schemas/OutputArtifactSetIdentity |  |
| <a id="s-261797bf17"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ac23675726"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3899c18b17"></a>`riverhog_disposition_set` | yes | #/components/schemas/ArtifactDispositionSetIdentity |  |
| <a id="s-196ffe30d1"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-488a513f4e"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field disposition_sha256](#s-74bb4956e0) | `length · characters · fixed` | shared above |
| [field job_id](#s-47b3af76c1) | `length · characters · fixed` | shared above |
| [field plan_sha256](#s-261797bf17) | `length · characters · fixed` | shared above |
| [field production_sha256](#s-ac23675726) | `length · characters · fixed` | shared above |
| [field source_edge_sha256](#s-488a513f4e) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionSetIdentity](schemas-artifactdispositionsetidentity.md)
- [schemas: OutputArtifactSetIdentity](schemas-outputartifactsetidentity.md)

## Governing policies

- <a id="pa-302891756e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-8c43dfe3af"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
