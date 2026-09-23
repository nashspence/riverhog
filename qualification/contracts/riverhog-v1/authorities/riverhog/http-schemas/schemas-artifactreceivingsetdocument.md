# schemas: ArtifactReceivingSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-artifactreceivingsetdocument:07282f0d32 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-2d2b977e07"></a>

- <a id="s-fd8f3baad6"></a>`type`: `"object"`
- <a id="s-12706c8a99"></a>`additionalProperties`: `false`
- <a id="s-eaf6966854"></a>`required`: `["state","count","total_bytes"]`
- <a id="s-8991498bb9"></a>`title`: `"ArtifactReceivingSetDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b4c4f8d76c"></a>`authority` | no | anyOf=[([ArtifactSetAuthorityDocument](schemas-artifactsetauthoritydocument.md)); (type="null")] |  |
| <a id="s-9eed34922f"></a>`count` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |
| <a id="s-69a169072e"></a>`state` | yes | type="string"; enum=["receiving","sealed"]; title="State" |  |
| <a id="s-97ef4d673d"></a>`total_bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |

## Maintained corroboration

### Referenced contract elements

- [ArtifactSetAuthorityDocument](schemas-artifactsetauthoritydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

- <a id="pa-b7d446ac01"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactReceivingSetDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f66af406cb1dafe7a60edc5a89eec2e66e324b4fe8308b0be876e9f63d52dfaa -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArtifactSetAuthorityDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "count": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    },
    "state": {
      "enum": [
        "receiving",
        "sealed"
      ],
      "title": "State",
      "type": "string"
    },
    "total_bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    }
  },
  "required": [
    "state",
    "count",
    "total_bytes"
  ],
  "title": "ArtifactReceivingSetDocument",
  "type": "object"
}
```

</details>
