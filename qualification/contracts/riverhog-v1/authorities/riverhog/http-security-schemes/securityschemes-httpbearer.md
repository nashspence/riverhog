# securitySchemes: HTTPBearer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-security-schemes:riverhog:securityschemes-httpbearer:6184389998 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Security Schemes](index.md) |

## External contract

<a id="s-344f2fc8db"></a>

| Field | Value |
|---|---|
| <a id="s-107e3a14b8"></a>`scheme` | `"bearer"` |
| <a id="s-c4be35f85d"></a>`type` | `"http"` |

## Governing policies

- <a id="pa-a8c9c22b69"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/securitySchemes/HTTPBearer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 178204f758fb78d572ed6d4673385f99c0e8697ced9fb775e7bb9851df3e06ee -->

```json
{
  "scheme": "bearer",
  "type": "http"
}
```

</details>
