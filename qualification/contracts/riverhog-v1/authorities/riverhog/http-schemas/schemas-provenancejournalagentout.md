# schemas: ProvenanceJournalAgentOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-provenancejournalagentout:6a94dcf2c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-929c31d612"></a>

- <a id="s-8a7765faaa"></a>`type`: `"object"`
- <a id="s-11130b26a9"></a>`additionalProperties`: `false`
- <a id="s-45d90a590c"></a>`required`: `["agent_id"]`
- <a id="s-1e7fff064e"></a>`title`: `"ProvenanceJournalAgentOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c50d626f42"></a>`agent_id` | yes | type="string"; title="Agent Id" |  |

## Governing policies

- <a id="pa-050aa1f59e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceJournalAgentOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f583f1b6cd0a63844e10b628bea016e3aa237fb18e598f3e20a7dd739a3d0151 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "agent_id": {
      "title": "Agent Id",
      "type": "string"
    }
  },
  "required": [
    "agent_id"
  ],
  "title": "ProvenanceJournalAgentOut",
  "type": "object"
}
```

</details>
