# securitySchemes: RiverhogFtpAdapterBearer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-security-schemes:riverhog-ftp-adapter:securityschemes-riverhogftpadapterbearer:314fa9148e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Security Schemes](index.md) |

## External contract

<a id="s-34e06de066"></a>
- <a id="s-b098781e12"></a>`type`: http

## Governing policies

- <a id="pa-b1b73dc9bc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/components/securitySchemes/RiverhogFtpAdapterBearer`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 178204f758fb78d572ed6d4673385f99c0e8697ced9fb775e7bb9851df3e06ee -->

```json
{
  "scheme": "bearer",
  "type": "http"
}
```
