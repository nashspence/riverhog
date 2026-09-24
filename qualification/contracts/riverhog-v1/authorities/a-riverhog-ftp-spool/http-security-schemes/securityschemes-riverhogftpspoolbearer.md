# securitySchemes: RiverhogFtpSpoolBearer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-security-schemes:a-riverhog-ftp-spool:securityschemes-riverhogftpspoolbearer:c02d6a424f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Security Schemes](index.md) |

## External contract

<a id="s-e9fe74a3d9"></a>

| Field | Value |
|---|---|
| <a id="s-ba5507fd7c"></a>`scheme` | `"bearer"` |
| <a id="s-5301abe095"></a>`type` | `"http"` |

## Governing policies

- <a id="pa-79b7fc4d8c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/components/securitySchemes/RiverhogFtpSpoolBearer`

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
