# schemas: FailedArchiveRootPublicationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-failedarchiverootpublicationout:3e25335494 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c27099525c"></a>
- <a id="s-a0e332b63f"></a>`title`: FailedArchiveRootPublicationOut
- <a id="s-80d3054b16"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-46b1e12b22"></a>`object_path` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-f70b18df4c"></a>`sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-b9cfcea2e8"></a>`state` | yes | type="string"; const="failed" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b1efcf232b"></a>[field sha256 · string value](#s-f70b18df4c) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-c502980b74"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-00a5667140"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/FailedArchiveRootPublicationOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6062323e444b0d20ceaf5f060e09481a418d2d0383d8054565dadc26b529123d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "object_path": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Object Path"
    },
    "sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Sha256"
    },
    "state": {
      "const": "failed",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state"
  ],
  "title": "FailedArchiveRootPublicationOut",
  "type": "object"
}
```
