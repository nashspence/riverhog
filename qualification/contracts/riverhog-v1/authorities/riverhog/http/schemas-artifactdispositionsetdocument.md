# schemas: ArtifactDispositionSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionsetdocument:208cd19fd9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-997fad8712"></a>
- <a id="s-a3fe5d1a54"></a>`title`: ArtifactDispositionSetDocument
- <a id="s-e15b82ce92"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3514768052"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8863e8a714"></a>`disposition_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-b0f5f1d2f0"></a>`failure` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| <a id="s-f610e69944"></a>`identity` | no | anyOf=#/components/schemas/ArtifactDispositionSetIdentityDocument \| type="null" |  |
| <a id="s-e06604fb79"></a>`output_artifact_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-242852ab10"></a>`output_edge_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-a339b730cb"></a>`state` | yes | type="string"; enum=["receiving","sealing","sealed","failed"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-3514768052) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-8c2a9e78c6"></a>[field failure · string value](#s-b0f5f1d2f0) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)

## Governing policies

- <a id="pa-130ced4e3e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-b243f0e180"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionSetDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 193ec959876d910dbd237e2dd02b3d7d30c9441335c81112f9d9631a1dc43523 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "disposition_count": {
      "minimum": 0,
      "title": "Disposition Count",
      "type": "integer"
    },
    "failure": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "identity": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArtifactDispositionSetIdentityDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "output_artifact_count": {
      "minimum": 0,
      "title": "Output Artifact Count",
      "type": "integer"
    },
    "output_edge_count": {
      "minimum": 0,
      "title": "Output Edge Count",
      "type": "integer"
    },
    "state": {
      "enum": [
        "receiving",
        "sealing",
        "sealed",
        "failed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "claim_id",
    "state",
    "disposition_count",
    "output_edge_count",
    "output_artifact_count"
  ],
  "title": "ArtifactDispositionSetDocument",
  "type": "object"
}
```
