# schemas: OmittedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-omittedfileprovenancebinding:25a1bef479 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-552d497babc9"></a>
- <a id="s-fb810a53030b"></a>`title`: OmittedFileProvenanceBinding
- <a id="s-7bbee14efc64"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b89eb94ed221"></a>`omission_reason` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| <a id="s-51b355044fcf"></a>`status` | yes | type="string"; const="omitted" |  |

## Governing policies

- <a id="pa-102a502e313e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedFileProvenanceBinding`

### Exact owned JSON

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
