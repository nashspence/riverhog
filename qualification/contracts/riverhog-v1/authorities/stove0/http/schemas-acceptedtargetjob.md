# schemas: AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-acceptedtargetjob:571531b8ae -->

Durable, non-secret identity of one accepted target job request.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-0b3d1b3480"></a>
- <a id="s-62ac17eb15"></a>`title`: AcceptedTargetJob
- <a id="s-ee57f72a82"></a>`description`: Durable, non-secret identity of one accepted target job request.
- <a id="s-b7f9ca6c75"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9dcf35deb6"></a>`declaration` | yes | #/components/schemas/TargetJobDeclaration |  |
| <a id="s-e846f6a178"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field request_sha256](#s-e846f6a178) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: TargetJobDeclaration](schemas-targetjobdeclaration.md)

## Governing policies

- <a id="pa-92fe252e7b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-69fa467187"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AcceptedTargetJob`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f00e108b9ab996f28a1dfd3928c40b9045b480dd5240737de55621c6ceb42bd -->

```json
{
  "additionalProperties": false,
  "description": "Durable, non-secret identity of one accepted target job request.",
  "properties": {
    "declaration": {
      "$ref": "#/components/schemas/TargetJobDeclaration"
    },
    "request_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Sha256",
      "type": "string"
    }
  },
  "required": [
    "declaration",
    "request_sha256"
  ],
  "title": "AcceptedTargetJob",
  "type": "object"
}
```
