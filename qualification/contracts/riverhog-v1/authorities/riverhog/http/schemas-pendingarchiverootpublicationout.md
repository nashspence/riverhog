# schemas: PendingArchiveRootPublicationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-pendingarchiverootpublicationout:d6504c7b47 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-14ed6c3d0b"></a>
- <a id="s-fbde03adef"></a>`title`: PendingArchiveRootPublicationOut
- <a id="s-a54c9801ab"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b2ae1a2f85"></a>`object_path` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-094900fe7d"></a>`sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-f04bdd7143"></a>`state` | no | type="string"; const="pending" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-40ff4376a0"></a>[field sha256 · string value](#s-094900fe7d) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-1c655c89a0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-29a0c5bde9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PendingArchiveRootPublicationOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 270f8f453df5bf9a03a87bc68ad94f0f932b5a11c6974579ec7bd7809c18a960 -->

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
      "const": "pending",
      "default": "pending",
      "title": "State",
      "type": "string"
    }
  },
  "title": "PendingArchiveRootPublicationOut",
  "type": "object"
}
```
