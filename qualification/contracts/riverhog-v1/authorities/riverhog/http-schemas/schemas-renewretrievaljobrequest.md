# schemas: RenewRetrievalJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-renewretrievaljobrequest:fd0a557c9f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-93df6fe639"></a>

- <a id="s-4aa08d60e7"></a>`type`: `"object"`
- <a id="s-dcbe19d757"></a>`additionalProperties`: `false`
- <a id="s-bf219bb720"></a>`required`: `["lease_seconds"]`
- <a id="s-2aece4cd02"></a>`title`: `"RenewRetrievalJobRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22f144242f"></a>`lease_seconds` | yes | type="integer"; minimum=1; title="Lease Seconds" |  |

## Governing policies

- <a id="pa-a081b649b6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RenewRetrievalJobRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c340f9b632598fe59aae3fc711a245ed0211b36c94b1b056c43b0a5bc95e419b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "lease_seconds": {
      "minimum": 1,
      "title": "Lease Seconds",
      "type": "integer"
    }
  },
  "required": [
    "lease_seconds"
  ],
  "title": "RenewRetrievalJobRequest",
  "type": "object"
}
```

</details>
