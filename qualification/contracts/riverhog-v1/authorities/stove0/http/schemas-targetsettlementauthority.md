# schemas: TargetSettlementAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetsettlementauthority:1928061221 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-428567adad"></a>
- <a id="s-6bb82eba89"></a>`title`: TargetSettlementAuthority
- <a id="s-49261af54d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ea9a996c0a"></a>`format` | no | type="string"; const="stove0-target-settlement/v1" |  |
| <a id="s-cde8f44573"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-55ad886411"></a>`output_bindings` | yes | #/components/schemas/TargetOutputBindingSetIdentity |  |
| <a id="s-4b257ebb6b"></a>`output_collection` | yes | #/components/schemas/OutputCollectionRef |  |
| <a id="s-525e441ceb"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3bfcddaed7"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field job_id](#s-cde8f44573) | `length · characters · fixed` | shared above |
| [field production_sha256](#s-525e441ceb) | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-3bfcddaed7) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: OutputCollectionRef](schemas-outputcollectionref.md)
- [schemas: TargetOutputBindingSetIdentity](schemas-targetoutputbindingsetidentity.md)

## Governing policies

- <a id="pa-f868984d33"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-e031b012bf"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetSettlementAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a811b1b88f435f8192083bd5bb9cab621322a012a9d17a39beebcbf46f0b2e02 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-target-settlement/v1",
      "default": "stove0-target-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "output_bindings": {
      "$ref": "#/components/schemas/TargetOutputBindingSetIdentity"
    },
    "output_collection": {
      "$ref": "#/components/schemas/OutputCollectionRef"
    },
    "production_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Production Sha256",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    }
  },
  "required": [
    "job_id",
    "production_sha256",
    "output_collection",
    "output_bindings",
    "settlement_sha256"
  ],
  "title": "TargetSettlementAuthority",
  "type": "object"
}
```
