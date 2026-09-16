# schemas: ProcessingClaimRestartDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimrestartdocument:556aa97bc1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6a5a871811"></a>

- <a id="s-ecbe707fb5"></a>`type`: `"object"`
- <a id="s-45f6b08a44"></a>`additionalProperties`: `false`
- <a id="s-902dd3833d"></a>`required`: `["fence"]`
- <a id="s-18dc3ae21e"></a>`title`: `"ProcessingClaimRestartDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba697e32c4"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-68ed410eac"></a>`lease_seconds` | no | type="integer"; minimum=30; maximum=86400; default=1800; title="Lease Seconds" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=86400; minimum=30; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field lease_seconds](#s-68ed410eac) | `value · schema-value · contract_max` | shared above |

## Governing policies

- <a id="pa-2c1ca94e64"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-b522f00eb8"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimRestartDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1567d5ffa38a300b1cbcb6ae7cf2fc435dc07e4f6a6b18d2578795929f17619 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "lease_seconds": {
      "default": 1800,
      "maximum": 86400,
      "minimum": 30,
      "title": "Lease Seconds",
      "type": "integer"
    }
  },
  "required": [
    "fence"
  ],
  "title": "ProcessingClaimRestartDocument",
  "type": "object"
}
```

</details>
