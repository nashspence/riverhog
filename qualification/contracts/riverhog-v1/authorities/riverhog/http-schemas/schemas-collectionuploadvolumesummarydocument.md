# schemas: CollectionUploadVolumeSummaryDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadvolumesummarydocument:c1a5bfff6a -->

Protocol-owned identity of one immutable collection archive volume.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3e0a34057d"></a>

- <a id="s-c189994b3b"></a>`type`: `"object"`
- <a id="s-59ea81ee4c"></a>`additionalProperties`: `false`
- <a id="s-d366f075da"></a>`description`: `"Protocol-owned identity of one immutable collection archive volume."`
- <a id="s-7504ebbc41"></a>`required`: `["volume_id","sequence","kind"]`
- <a id="s-956f2b8a24"></a>`title`: `"CollectionUploadVolumeSummaryDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7130580c64"></a>`kind` | yes | type="string"; enum=["pack","segment"]; title="Kind" |  |
| <a id="s-34ae7e2e00"></a>`sequence` | yes | [Sequence256Hex](schemas-sequence256hex.md) |  |
| <a id="s-1329a434a5"></a>`volume_id` | yes | type="string"; pattern="^(?:pack\|segment)-[0-9a-f]{64}$"; title="Volume Id" |  |

## Maintained corroboration

### Referenced contract elements

- [Sequence256Hex](schemas-sequence256hex.md)

## Governing policies

- <a id="pa-a7748535ce"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadVolumeSummaryDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 972a6d63bd342dc5ee15fd86187faaab19ec45e306358b94acb6455cff8e8113 -->

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
      "$ref": "#/components/schemas/Sequence256Hex"
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

</details>
