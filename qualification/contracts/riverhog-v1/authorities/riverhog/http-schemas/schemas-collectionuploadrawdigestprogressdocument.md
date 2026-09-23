# schemas: CollectionUploadRawDigestProgressDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadrawdigestprogressdocument:7c4a9cd34b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-af655bffa6"></a>

- <a id="s-eee2c580c6"></a>`type`: `"object"`
- <a id="s-3c1efd7ed5"></a>`additionalProperties`: `false`
- <a id="s-8b015bc816"></a>`required`: `["path","accepted_parts","expected_parts","complete"]`
- <a id="s-0ebbaa177b"></a>`title`: `"CollectionUploadRawDigestProgressDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-357edf47b9"></a>`accepted_parts` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md) |  |
| <a id="s-d4a25895d4"></a>`complete` | yes | type="boolean"; title="Complete" |  |
| <a id="s-87d83509ec"></a>`expected_parts` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-1e7da99a8e"></a>`path` | yes | type="string"; title="Path" |  |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

- <a id="pa-6a749fdc77"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRawDigestProgressDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d9e8da190d28b3ccb7d962d5a124d7efce8f17ecc6c6adba1bdefc15cc6d013 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "accepted_parts": {
      "$ref": "#/components/schemas/NonnegativeDecimal"
    },
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "expected_parts": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "path": {
      "title": "Path",
      "type": "string"
    }
  },
  "required": [
    "path",
    "accepted_parts",
    "expected_parts",
    "complete"
  ],
  "title": "CollectionUploadRawDigestProgressDocument",
  "type": "object"
}
```

</details>
