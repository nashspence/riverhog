# schemas: CollectionDeletionArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiondeletionarchivecopyout:ea630e1e12 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3af3d570eb"></a>

- <a id="s-18e3bfc95c"></a>`type`: `"object"`
- <a id="s-29c3879c7d"></a>`additionalProperties`: `false`
- <a id="s-a539d9ffcc"></a>`required`: `["store","objects","stored_bytes"]`
- <a id="s-954c713a38"></a>`title`: `"CollectionDeletionArchiveCopyOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bfeff2016"></a>`objects` | yes | type="integer"; title="Objects" |  |
| <a id="s-096c6e325d"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-b0b6a85548"></a>`stored_bytes` | yes | type="integer"; title="Stored Bytes" |  |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- <a id="pa-80dc382b57"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletionArchiveCopyOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42d4bdcc75153940ccad496bbce0aa33749ab018e46dded25dddf95544b3db51 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "objects": {
      "title": "Objects",
      "type": "integer"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    }
  },
  "required": [
    "store",
    "objects",
    "stored_bytes"
  ],
  "title": "CollectionDeletionArchiveCopyOut",
  "type": "object"
}
```

</details>
