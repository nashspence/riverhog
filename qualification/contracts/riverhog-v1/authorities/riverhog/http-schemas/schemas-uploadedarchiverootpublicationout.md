# schemas: UploadedArchiveRootPublicationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-uploadedarchiverootpublicationout:4b08875fee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-0e94bb0a8d"></a>

- <a id="s-fced31efe7"></a>`type`: `"object"`
- <a id="s-7a15e63a87"></a>`additionalProperties`: `false`
- <a id="s-ea96a1f280"></a>`required`: `["object_path","sha256","state"]`
- <a id="s-a34412b459"></a>`title`: `"UploadedArchiveRootPublicationOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1038f52929"></a>`object_path` | yes | type="string"; minLength=1; title="Object Path" |  |
| <a id="s-0ebf12c9cf"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-12993faaaa"></a>`state` | yes | type="string"; const="uploaded"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-0ebf12c9cf) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-bfbbbd186c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-4e5691949a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/UploadedArchiveRootPublicationOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
