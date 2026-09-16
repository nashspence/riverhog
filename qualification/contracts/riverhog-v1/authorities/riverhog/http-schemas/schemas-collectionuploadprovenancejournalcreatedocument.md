# schemas: CollectionUploadProvenanceJournalCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadprovenancejournal-8b3d885c5e:d0e18a6924 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-cd0f4e795b"></a>

- <a id="s-f655cdfeb2"></a>`type`: `"object"`
- <a id="s-46a5b70044"></a>`additionalProperties`: `false`
- <a id="s-9acf07de71"></a>`required`: `["bytes","sha256"]`
- <a id="s-96fbc31b79"></a>`title`: `"CollectionUploadProvenanceJournalCreateDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-14e21bbc93"></a>`bytes` | yes | type="integer"; minimum=1; title="Bytes" |  |
| <a id="s-751cbd4013"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-14e21bbc93) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-751cbd4013) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-92f47d13b2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-64dd1a835d"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-0f5ff0fb69"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadProvenanceJournalCreateDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44d861666e69c14f866d6b8948c4fc7a9c7a1534b331276b15a5ec2117e26cf9 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 1,
      "title": "Bytes",
      "type": "integer"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "bytes",
    "sha256"
  ],
  "title": "CollectionUploadProvenanceJournalCreateDocument",
  "type": "object"
}
```

</details>
