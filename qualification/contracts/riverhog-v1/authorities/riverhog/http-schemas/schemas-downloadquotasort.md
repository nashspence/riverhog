# schemas: DownloadQuotaSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-downloadquotasort:6e66425875 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-581931c55e"></a>

- <a id="s-73028c671b"></a>`type`: `"string"`
- <a id="s-522a22c4de"></a>`enum`: `["app","key_id","monthly_bytes","accounted_bytes","reserved_bytes","remaining_bytes"]`

## Governing policies

- <a id="pa-1e6f269cbe"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/DownloadQuotaSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d52260d58d8d4e3379e7edad928477b9e6720262b0503c94f376d5a0411f5cc -->

```json
{
  "enum": [
    "app",
    "key_id",
    "monthly_bytes",
    "accounted_bytes",
    "reserved_bytes",
    "remaining_bytes"
  ],
  "type": "string"
}
```

</details>
