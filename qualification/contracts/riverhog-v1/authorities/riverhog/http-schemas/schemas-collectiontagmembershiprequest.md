# schemas: CollectionTagMembershipRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiontagmembershiprequest:d6da3d7cc6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-dcbd49897c"></a>

- <a id="s-df23e91fd7"></a>`type`: `"object"`
- <a id="s-fae0d779df"></a>`additionalProperties`: `false`
- <a id="s-5bcd0c4953"></a>`required`: `["tag"]`
- <a id="s-f574686c3f"></a>`title`: `"CollectionTagMembershipRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d7b2f80e3c"></a>`tag` | yes | [CollectionTag](schemas-collectiontag.md) |  |

## Maintained corroboration

### Referenced contract elements

- [CollectionTag](schemas-collectiontag.md)

## Governing policies

- <a id="pa-59dcd5e0c4"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagMembershipRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5d169df230e0ccca5a2b6667ba55a41426fad09e33c244dbf9f33a0be220fdd -->

```json
{
  "additionalProperties": false,
  "properties": {
    "tag": {
      "$ref": "#/components/schemas/CollectionTag"
    }
  },
  "required": [
    "tag"
  ],
  "title": "CollectionTagMembershipRequest",
  "type": "object"
}
```

</details>
