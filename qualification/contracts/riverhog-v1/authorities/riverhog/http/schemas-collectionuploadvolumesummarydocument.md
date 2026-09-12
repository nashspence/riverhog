# schemas: CollectionUploadVolumeSummaryDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadvolumesummarydocument:659de219af -->

Protocol-owned identity of one immutable collection archive volume.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3e0a34057da3"></a>
- <a id="s-956f2b8a2492"></a>`title`: CollectionUploadVolumeSummaryDocument
- <a id="s-d366f075da47"></a>`description`: Protocol-owned identity of one immutable collection archive volume.
- <a id="s-c189994b3b40"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7130580c64de"></a>`kind` | yes | type="string"; enum=["pack","segment"] |  |
| <a id="s-34ae7e2e0057"></a>`sequence` | yes | type="integer"; minimum=0 |  |
| <a id="s-1329a434a507"></a>`volume_id` | yes | type="string"; pattern="^(?:pack\|segment)-[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-7e2c0bf38865"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadVolumeSummaryDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ac7efaa0bbffcab4fff943fbbab4f3ed050c499a9fe1520ac38e1ccb0e6d859 -->

```json
{
  "additionalProperties": false,
  "description": "Protocol-owned identity of one immutable collection archive volume.",
  "properties": {
    "kind": {
      "enum": [
        "pack",
        "segment"
      ],
      "title": "Kind",
      "type": "string"
    },
    "sequence": {
      "minimum": 0,
      "title": "Sequence",
      "type": "integer"
    },
    "volume_id": {
      "pattern": "^(?:pack|segment)-[0-9a-f]{64}$",
      "title": "Volume Id",
      "type": "string"
    }
  },
  "required": [
    "volume_id",
    "sequence",
    "kind"
  ],
  "title": "CollectionUploadVolumeSummaryDocument",
  "type": "object"
}
```
