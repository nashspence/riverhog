# schemas: CompletionFailureStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:a-riverhog-ftp-spool:schemas-completionfailurestatus:74c1459574 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-22424d9591"></a>

- <a id="s-0d12734dba"></a>`type`: `"object"`
- <a id="s-07d9a51dc1"></a>`additionalProperties`: `false`
- <a id="s-ba1eee2b24"></a>`required`: `["reason","retryable","attempts"]`
- <a id="s-bfca75237b"></a>`title`: `"CompletionFailureStatus"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-16bec83129"></a>`attempts` | yes | type="integer"; minimum=0; title="Attempts" |  |
| <a id="s-c43c8b9f4c"></a>`reason` | yes | type="string"; title="Reason" |  |
| <a id="s-15b3a3aa87"></a>`retryable` | yes | type="boolean"; title="Retryable" |  |

## Governing policies

- <a id="pa-c0e9691b1c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/schemas/CompletionFailureStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74cbbb77e8ea41097c95c5e35a2e55f1174d980d9cbf1f09313ee64e6a36e00e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "attempts": {
      "minimum": 0,
      "title": "Attempts",
      "type": "integer"
    },
    "reason": {
      "title": "Reason",
      "type": "string"
    },
    "retryable": {
      "title": "Retryable",
      "type": "boolean"
    }
  },
  "required": [
    "reason",
    "retryable",
    "attempts"
  ],
  "title": "CompletionFailureStatus",
  "type": "object"
}
```

</details>
