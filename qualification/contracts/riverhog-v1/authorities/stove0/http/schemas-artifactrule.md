# schemas: ArtifactRule

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactrule:1d9ed84d81 -->

Classify one path; first matching rule wins.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ab5708cfcb23"></a>
- <a id="s-49029b76158e"></a>`title`: ArtifactRule
- <a id="s-edfa6cde0a61"></a>`description`: Classify one path; first matching rule wins.
- <a id="s-a8acaa94a4fb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6f5087153a1d"></a>`glob` | no | type="string" |  |
| <a id="s-14f9256789ae"></a>`media_type` | no | anyOf=type="string" \| type="null" |  |
| <a id="s-041330fca4c8"></a>`role` | no | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Governing policies

- <a id="pa-3469dc17ecdc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactRule`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3392b5b6ba296932dd8651fd6ad0532db4a7cf5bf186fb0991311dc91d753a1d -->

```json
{
  "additionalProperties": false,
  "description": "Classify one path; first matching rule wins.",
  "properties": {
    "glob": {
      "default": "*",
      "title": "Glob",
      "type": "string"
    },
    "media_type": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Media Type"
    },
    "role": {
      "default": "stove0.source/v1",
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Role",
      "type": "string"
    }
  },
  "title": "ArtifactRule",
  "type": "object"
}
```
