# schemas: OmittedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-omittedfileprovenancebinding:c353621d28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-552d497bab"></a>

- <a id="s-7bbee14efc"></a>`type`: `"object"`
- <a id="s-f8e822b26f"></a>`additionalProperties`: `false`
- <a id="s-514bd92d58"></a>`required`: `["status","omission_reason"]`
- <a id="s-fb810a5303"></a>`title`: `"OmittedFileProvenanceBinding"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b89eb94ed2"></a>`omission_reason` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"; title="Omission Reason" |  |
| <a id="s-51b355044f"></a>`status` | yes | type="string"; const="omitted"; title="Status" |  |

## Governing policies

- <a id="pa-646d50bc95"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedFileProvenanceBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5eba6adbe60f961048cf4ff31b645ca43fb22f9f4ff95b3476666b75a05b8a3c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "omission_reason": {
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Omission Reason",
      "type": "string"
    },
    "status": {
      "const": "omitted",
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "status",
    "omission_reason"
  ],
  "title": "OmittedFileProvenanceBinding",
  "type": "object"
}
```

</details>
