# schemas: ProvenanceTraceExternalStateReferenceItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-provenancetraceexternalstatereferenceitemout:fbc4e42926 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8a60d61b30"></a>

- <a id="s-63e947ccd8"></a>`type`: `"object"`
- <a id="s-004d028798"></a>`additionalProperties`: `false`
- <a id="s-dc19eac5b6"></a>`required`: `["kind","reference"]`
- <a id="s-46aa0a8a0c"></a>`title`: `"ProvenanceTraceExternalStateReferenceItemOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-82c2fe1e31"></a>`kind` | yes | type="string"; const="external_state_reference"; title="Kind" |  |
| <a id="s-ac9f21d08d"></a>`reference` | yes | [ProvenanceExternalStateReferenceOut](schemas-provenanceexternalstatereferenceout.md) |  |

## Maintained corroboration

### Referenced contract dossiers

- [ProvenanceExternalStateReferenceOut](schemas-provenanceexternalstatereferenceout.md)

## Governing policies

- <a id="pa-d99048c358"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProvenanceTraceExternalStateReferenceItemOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14775eb0d4995fb0d8d72c90bed4aef8f5075c75e67f45ac5a8720eaf6eb1e67 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "kind": {
      "const": "external_state_reference",
      "title": "Kind",
      "type": "string"
    },
    "reference": {
      "$ref": "#/components/schemas/ProvenanceExternalStateReferenceOut"
    }
  },
  "required": [
    "kind",
    "reference"
  ],
  "title": "ProvenanceTraceExternalStateReferenceItemOut",
  "type": "object"
}
```

</details>
