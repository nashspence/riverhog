# schemas: UploadedArchiveRootPublicationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-uploadedarchiverootpublicationout:a75186572a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-0e94bb0a8d1e"></a>
- <a id="s-a34412b459a5"></a>`title`: UploadedArchiveRootPublicationOut
- <a id="s-fced31efe7aa"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1038f52929c2"></a>`object_path` | yes | type="string"; minLength=1 |  |
| <a id="s-0ebf12c9cf35"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-12993faaaa26"></a>`state` | yes | type="string"; const="uploaded" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-0ebf12c9cf35) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-db780051e3d7"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-453aa2d38219"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/UploadedArchiveRootPublicationOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09c985b2a04103fb9074371778c6f08cf50ee3306070442f52193a4f7e8db5cf -->

```json
{
  "additionalProperties": false,
  "properties": {
    "object_path": {
      "minLength": 1,
      "title": "Object Path",
      "type": "string"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    },
    "state": {
      "const": "uploaded",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "object_path",
    "sha256",
    "state"
  ],
  "title": "UploadedArchiveRootPublicationOut",
  "type": "object"
}
```
