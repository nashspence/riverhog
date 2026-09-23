# schemas: ArchiveCopyJobState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobstate:1786b5244d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6b2f71a875"></a>

- <a id="s-fe08beca6f"></a>`type`: `"string"`
- <a id="s-f7b00476d2"></a>`enum`: `["requested","waiting","checking","copying","canceling","completed","failed","canceled"]`

## Governing policies

- <a id="pa-607a8f9955"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fef2e66a0ed103e056d143a56b935cdc4bf167947bf6809ceaa7da45027e7d18 -->

```json
{
  "enum": [
    "requested",
    "waiting",
    "checking",
    "copying",
    "canceling",
    "completed",
    "failed",
    "canceled"
  ],
  "type": "string"
}
```

</details>
