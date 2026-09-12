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

<a id="s-997fad8712ae"></a>
- <a id="s-a3fe5d1a54dd"></a>`title`: ArtifactDispositionSetDocument
- <a id="s-e15b82ce92ea"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-351476805296"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8863e8a714e8"></a>`disposition_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-b0f5f1d2f06b"></a>`failure` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| <a id="s-f610e6994497"></a>`identity` | no | anyOf=#/components/schemas/ArtifactDispositionSetIdentityDocument \| type="null" |  |
| <a id="s-e06604fb7965"></a>`output_artifact_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-242852ab10c8"></a>`output_edge_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-a339b730cb3f"></a>`state` | yes | type="string"; enum=["receiving","sealing","sealed","failed"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-351476805296) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-8c2a9e78c623"></a>field failure · anyOf alternative 1 | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)

## Governing policies

- <a id="pa-130ced4e3ef8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-b243f0e1802b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
